import datetime
import hashlib
import json
import random
import re
import uuid
from collections.abc import Generator, Iterator
from typing import Any, TypeVar

import click
import petname
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.flight as flight
import query_farm_flight_server.auth as auth
import query_farm_flight_server.auth_manager as auth_manager
import query_farm_flight_server.auth_manager_naive as auth_manager_naive
import query_farm_flight_server.flight_handling as flight_handling
import query_farm_flight_server.flight_inventory as flight_inventory
import query_farm_flight_server.middleware as base_middleware
import query_farm_flight_server.parameter_types as parameter_types
import query_farm_flight_server.server as base_server
import structlog
from pydantic import BaseModel, ConfigDict, field_serializer, field_validator

from .database_impl import (
    DatabaseLibraryContext,
    descriptor_unpack_,
)

adjectives = [
    "happy",
    "brave",
    "fuzzy",
    "sleepy",
    "gentle",
    "silly",
    "zesty",
    "bouncy",
    "sunny",
    "tiny",
    "fluffy",
    "cozy",
    "cheery",
    "jazzy",
    "plucky",
    "quirky",
]

animals = [
    "otter",
    "koala",
    "penguin",
    "sloth",
    "fox",
    "hedgehog",
    "panda",
    "llama",
    "kitten",
    "puppy",
    "duckling",
    "owl",
    "beaver",
    "chick",
    "turtle",
    "goose",
]


def uuid_to_name(u: str) -> str:
    # Get a stable hash digest
    digest = hashlib.sha256(u.encode()).digest()

    # Convert bytes to two indices deterministically
    adj_index = digest[0] % len(adjectives)
    animal_index = digest[1] % len(animals)

    return f"{adjectives[adj_index]}-{animals[animal_index]}"


log = structlog.get_logger()


def read_recordbatch(source: bytes) -> pa.RecordBatch:
    """
    Read a record batch from a byte string.
    """
    buffer = pa.BufferReader(source)
    ipc_stream = pa.ipc.open_stream(buffer)
    return next(ipc_stream)


def conform_nullable(schema: pa.Schema, table: pa.Table) -> pa.Table:
    """
    Conform the table to the nullable flags as defined in the schema.

    There shouldn't be null values in the columns.

    This is needed because DuckDB doesn't send the nullable flag in the schema
    it sends via the DoExchange call.
    """
    for idx, table_field in enumerate(schema):
        if not table_field.nullable:
            # Only update the column if the new schema allows nulls where the original did not
            new_field = table_field.with_nullable(False)

            # Check for null values.
            if table.column(idx).null_count > 0:
                raise flight.FlightServerError(
                    f"Column {table_field.name} has null values, but the schema does not allow nulls."
                )

            table = table.set_column(idx, new_field, table.column(idx))
    return table


def check_schema_is_subset_of_schema(existing_schema: pa.Schema, new_schema: pa.Schema) -> None:
    """
    Check that the new schema is a subset of the existing schema.
    """
    existing_contents = set([(field.name, field.type) for field in existing_schema])
    new_contents = set([(field.name, field.type) for field in new_schema])

    unknown_fields = new_contents - existing_contents
    if unknown_fields:
        raise flight.FlightServerError(f"Unknown fields in insert: {unknown_fields}")
    return


class FlightTicketData(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)  # for Pydantic v2
    descriptor: flight.FlightDescriptor

    _validate_flight_descriptor = field_validator("descriptor", mode="before")(
        parameter_types.deserialize_flight_descriptor
    )

    @field_serializer("descriptor")
    def serialize_flight_descriptor(self, value: flight.FlightDescriptor, info: Any) -> bytes:
        return parameter_types.serialize_flight_descriptor(value, info)


T = TypeVar("T", bound=BaseModel)


class InMemoryArrowFlightServer(base_server.BasicFlightServer[auth.Account, auth.AccountToken]):
    def __init__(
        self,
        *,
        location: str | None,
        auth_manager: auth_manager.AuthManager[auth.Account, auth.AccountToken],
        **kwargs: dict[str, Any],
    ) -> None:
        self.service_name = "hello_world_server"
        self._auth_manager = auth_manager
        super().__init__(location=location, **kwargs)

        self.ROWID_FIELD_NAME = "rowid"
        self.rowid_field = pa.field(self.ROWID_FIELD_NAME, pa.int64(), metadata={"is_rowid": "1"})

    def action_endpoints(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        parameters: parameter_types.Endpoints,
    ) -> list[flight.FlightEndpoint]:
        descriptor_parts = descriptor_unpack_(parameters.descriptor)
        with DatabaseLibraryContext(readonly=True) as library:
            database = library.by_name(descriptor_parts.catalog_name)
            schema = database.by_name(descriptor_parts.schema_name)

            if descriptor_parts.type == "table":
                table_info = schema.by_name("table", descriptor_parts.name)

                ticket_data = FlightTicketData(
                    descriptor=parameters.descriptor,
                )

                if table_info.endpoint_generator is not None:
                    return table_info.endpoint_generator(ticket_data)
                return [flight_handling.endpoint(ticket_data=ticket_data, locations=None)]
            else:
                raise flight.FlightServerError(f"Unsupported descriptor type: {descriptor_parts.type}")

    def action_list_schemas(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        parameters: parameter_types.ListSchemas,
    ) -> base_server.AirportSerializedCatalogRoot:
        with DatabaseLibraryContext(readonly=True) as library:
            database = library.by_name(parameters.catalog_name)

            dynamic_inventory: dict[str, dict[str, list[flight_inventory.FlightInventoryWithMetadata]]] = {}

            catalog_contents = dynamic_inventory.setdefault(parameters.catalog_name, {})

            for schema_name, schema in database.schemas_by_name.items():
                schema_contents = catalog_contents.setdefault(schema_name, [])
                for coll in schema.containers():
                    for name, obj in coll.items():
                        schema_contents.append(
                            obj.flight_info(
                                name=name,
                                catalog_name=parameters.catalog_name,
                                schema_name=schema_name,
                            )
                        )

            return flight_inventory.upload_and_generate_schema_list(
                upload_parameters=flight_inventory.UploadParameters(
                    s3_client=None,
                    base_url="http://localhost:53333",
                    bucket_name="test_bucket",
                    bucket_prefix="test_prefix",
                ),
                flight_service_name=self.service_name,
                flight_inventory=dynamic_inventory,
                schema_details={},
                skip_upload=True,
                serialize_inline=True,
                catalog_version=1,
                catalog_version_fixed=False,
            )

    def impl_list_flights(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        criteria: bytes,
    ) -> Iterator[flight.FlightInfo]:
        with DatabaseLibraryContext(readonly=True) as library:

            def yield_flight_infos() -> Generator[flight.FlightInfo, None, None]:
                for db_name, db in library.databases_by_name.items():
                    for schema_name, schema in db.schemas_by_name.items():
                        for coll in schema.containers():
                            for name, obj in coll.items():
                                yield obj.flight_info(name=name, catalog_name=db_name, schema_name=schema_name)[0]

            return yield_flight_infos()

    def impl_get_flight_info(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        descriptor: flight.FlightDescriptor,
    ) -> flight.FlightInfo:
        descriptor_parts = descriptor_unpack_(descriptor)
        with DatabaseLibraryContext(readonly=True) as library:
            database = library.by_name(descriptor_parts.catalog_name)
            schema = database.by_name(descriptor_parts.schema_name)

            obj = schema.by_name(descriptor_parts.type, descriptor_parts.name)
            return obj.flight_info(
                name=descriptor_parts.name,
                catalog_name=descriptor_parts.catalog_name,
                schema_name=descriptor_parts.schema_name,
            )[0]

    def action_catalog_version(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        parameters: parameter_types.CatalogVersion,
    ) -> base_server.GetCatalogVersionResult:
        return base_server.GetCatalogVersionResult(catalog_version=1, is_fixed=True)

    def action_create_transaction(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        parameters: parameter_types.CreateTransaction,
    ) -> base_server.CreateTransactionResult:
        return base_server.CreateTransactionResult(identifier=None)

    def exchange_update(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        descriptor: flight.FlightDescriptor,
        reader: flight.MetadataRecordBatchReader,
        writer: flight.MetadataRecordBatchWriter,
        return_chunks: bool,
    ) -> int:
        descriptor_parts = descriptor_unpack_(descriptor)

        if descriptor_parts.type != "table":
            raise flight.FlightServerError(f"Unsupported descriptor type: {descriptor_parts.type}")

        with DatabaseLibraryContext() as library:
            database = library.by_name(descriptor_parts.catalog_name)
            schema = database.by_name(descriptor_parts.schema_name)
            table_info = schema.by_name("table", descriptor_parts.name)

            if table_info.frozen:
                raise flight.FlightServerError("Cannot delete from a frozen table")

            existing_table = table_info.version()

            writer.begin(existing_table.schema)

            rowid_index = existing_table.schema.get_field_index(self.ROWID_FIELD_NAME)
            assert rowid_index != -1

            change_count = 0

            for chunk in reader:
                if chunk.data is not None:
                    chunk_table = pa.Table.from_batches([chunk.data])
                    assert chunk_table.num_rows > 0

                    # So this chunk will contain any updated columns and the row id.

                    input_rowid_index = chunk_table.schema.get_field_index(self.ROWID_FIELD_NAME)

                    # To perform an update, first remove the rows from the table,
                    # then assign new row ids to the incoming rows, and append them to the table.
                    table_mask = pc.is_in(
                        existing_table.column(rowid_index),
                        value_set=chunk_table.column(input_rowid_index),
                    )

                    # This is the table with updated rows removed, we'll be adding rows to it later on.
                    table_without_updated_rows = pc.filter(existing_table, pc.invert(table_mask))

                    # These are the rows that are being updated, since the update may not send all
                    # the columns, we need to filter the table to get the updated rows to persist the
                    # values that aren't being updated.
                    changed_rows = pc.filter(existing_table, table_mask)

                    header_middleware = context.context.get_middleware("headers")
                    assert header_middleware
                    airport_client_session_id = header_middleware.client_headers.get("airport-client-session-id")

                    if airport_client_session_id is None or len(airport_client_session_id) == 0:
                        raise flight.FlightServerError("Missing airport-client-session-id header")

                    sender_name = uuid_to_name(airport_client_session_id[0])

                    if any(filter(lambda v: v != sender_name, changed_rows["sender"].to_pylist())) > 0:
                        raise flight.FlightServerError(
                            "Cannot update rows with a different sender than the original row."
                        )

                    # Get a list of all of the columns that are not being updated, so that a join
                    # can be performed.
                    unchanged_column_names = set(existing_table.schema.names) - set(chunk_table.schema.names)

                    joined_table = pa.Table.join(
                        changed_rows.select(list(unchanged_column_names) + [self.ROWID_FIELD_NAME]),
                        chunk_table,
                        keys=[self.ROWID_FIELD_NAME],
                        join_type="inner",
                    )

                    # Add the new row id column.
                    chunk_length = len(joined_table)
                    rowid_values = [
                        x
                        for x in range(
                            table_info.row_id_counter,
                            table_info.row_id_counter + chunk_length,
                        )
                    ]
                    updated_rows = joined_table.set_column(
                        joined_table.schema.get_field_index(self.ROWID_FIELD_NAME),
                        self.rowid_field,
                        [rowid_values],
                    )
                    table_info.row_id_counter += chunk_length

                    # Now the columns may be in a different order, so we need to realign them.
                    updated_rows = updated_rows.select(existing_table.schema.names)

                    check_schema_is_subset_of_schema(existing_table.schema, updated_rows.schema)

                    updated_rows = conform_nullable(existing_table.schema, updated_rows)

                    updated_table = pa.concat_tables(
                        [
                            table_without_updated_rows,
                            updated_rows.select(table_without_updated_rows.schema.names),
                        ]
                    )

                    if return_chunks:
                        writer.write_table(updated_rows)

                    existing_table = updated_table

            table_info.update_table(existing_table)

        return change_count

    def exchange_delete(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        descriptor: flight.FlightDescriptor,
        reader: flight.MetadataRecordBatchReader,
        writer: flight.MetadataRecordBatchWriter,
        return_chunks: bool,
    ) -> int:
        descriptor_parts = descriptor_unpack_(descriptor)

        if descriptor_parts.type != "table":
            raise flight.FlightServerError(f"Unsupported descriptor type: {descriptor_parts.type}")
        with DatabaseLibraryContext() as library:
            database = library.by_name(descriptor_parts.catalog_name)
            schema = database.by_name(descriptor_parts.schema_name)
            table_info = schema.by_name("table", descriptor_parts.name)

            if table_info.frozen:
                raise flight.FlightServerError("Cannot delete from a frozen table")

            existing_table = table_info.version()
            writer.begin(existing_table.schema)

            rowid_index = existing_table.schema.get_field_index(self.ROWID_FIELD_NAME)
            assert rowid_index != -1

            change_count = 0

            for chunk in reader:
                if chunk.data is not None:
                    chunk_table = pa.Table.from_batches([chunk.data])
                    assert chunk_table.num_rows > 0

                    # Should only be getting the row id.
                    assert chunk_table.num_columns == 1
                    # the rowid field doesn't come back the same since it missing the
                    # not null flag and the metadata, so can't compare the schemas
                    input_rowid_index = chunk_table.schema.get_field_index(self.ROWID_FIELD_NAME)

                    # Now do an antijoin to get the rows that are not in the delete_rows.
                    target_rowids = chunk_table.column(input_rowid_index)
                    existing_row_ids = existing_table.column(rowid_index)

                    mask = pc.is_in(existing_row_ids, value_set=target_rowids)

                    target_rows = pc.filter(existing_table, mask)

                    header_middleware = context.context.get_middleware("headers")
                    assert header_middleware
                    airport_client_session_id = header_middleware.client_headers.get("airport-client-session-id")

                    if airport_client_session_id is None or len(airport_client_session_id) == 0:
                        raise flight.FlightServerError("Missing airport-client-session-id header")

                    sender_name = uuid_to_name(airport_client_session_id[0])

                    if any(filter(lambda v: v != sender_name, target_rows["sender"].to_pylist())) > 0:
                        raise flight.FlightServerError(
                            "Cannot delete rows with a different sender than the original row."
                        )

                    changed_table = pc.filter(existing_table, pc.invert(mask))

                    change_count += target_rows.num_rows

                    if return_chunks:
                        writer.write_table(target_rows)

                    existing_table = changed_table

            table_info.update_table(existing_table)

        return change_count

    def exchange_insert(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        descriptor: flight.FlightDescriptor,
        reader: flight.MetadataRecordBatchReader,
        writer: flight.MetadataRecordBatchWriter,
        return_chunks: bool,
    ) -> int:
        descriptor_parts = descriptor_unpack_(descriptor)

        if descriptor_parts.type != "table":
            raise flight.FlightServerError(f"Unsupported descriptor type: {descriptor_parts.type}")

        with DatabaseLibraryContext() as library:
            database = library.by_name(descriptor_parts.catalog_name)
            schema = database.by_name(descriptor_parts.schema_name)
            table_info = schema.by_name("table", descriptor_parts.name)

            if table_info.frozen:
                raise flight.FlightServerError("Cannot delete from a frozen table")

            existing_table = table_info.version()
            writer.begin(existing_table.schema)
            change_count = 0

            rowid_index = existing_table.schema.get_field_index(self.ROWID_FIELD_NAME)
            assert rowid_index != -1

            # Check that the data being read matches the table without the rowid column.

            # DuckDB won't send field metadata when it sends us the schema that it uses
            # to perform an insert, so we need some way to adapt the schema we
            check_schema_is_subset_of_schema(existing_table.schema, reader.schema)

            # FIXME: need to handle the case of rowids.

            total_insert_rows = 0
            for chunk in reader:
                if chunk.data is not None:
                    new_rows = pa.Table.from_batches([chunk.data])

                    total_insert_rows += new_rows.num_rows
                    if total_insert_rows > 1:
                        raise flight.FlightServerError("Inserts must be done one row at a time to the chat table.")

                    new_row = new_rows.to_pylist()[0]
                    new_row["timestamp"] = datetime.datetime.now(datetime.timezone.utc)

                    header_middleware = context.context.get_middleware("headers")
                    assert header_middleware
                    airport_client_session_id = header_middleware.client_headers.get("airport-client-session-id")

                    if airport_client_session_id is None or len(airport_client_session_id) == 0:
                        raise flight.FlightServerError("Missing airport-client-session-id header")

                    new_row["id"] = table_info.row_id_counter
                    table_info.row_id_counter += 1
                    new_row["sender"] = uuid_to_name(airport_client_session_id[0])

                    if new_row.get("channel") is None:
                        raise flight.FlightServerError("Missing channel in insert row")

                    new_rows = pa.Table.from_pylist(
                        [new_row],
                        schema=pa.schema(
                            [
                                pa.field("id", pa.int64()),
                                pa.field("sender", pa.string()),
                                pa.field("message", pa.string()),
                                pa.field("timestamp", pa.timestamp("ms")),
                                pa.field("channel", pa.string()),
                            ]
                        ),
                    )

                    assert new_rows.num_rows > 0

                    # append the row id column to the new rows.
                    chunk_length = new_rows.num_rows
                    rowid_values = [
                        x
                        for x in range(
                            table_info.row_id_counter,
                            table_info.row_id_counter + chunk_length,
                        )
                    ]
                    new_rows = new_rows.append_column(self.rowid_field, [rowid_values])
                    table_info.row_id_counter += chunk_length
                    change_count += chunk_length

                    if return_chunks:
                        writer.write_table(new_rows)

                    new_rows = conform_nullable(existing_table.schema, new_rows)

                    existing_table = pa.concat_tables([existing_table, new_rows.select(existing_table.schema.names)])

            table_info.update_table(existing_table.slice(max(0, existing_table.num_rows - 25)))

        return change_count

    def action_column_statistics(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        parameters: parameter_types.ColumnStatistics,
    ) -> pa.Table:
        descriptor_parts = descriptor_unpack_(parameters.flight_descriptor)
        with DatabaseLibraryContext() as library:
            database = library.by_name(descriptor_parts.catalog_name)
            schema = database.by_name(descriptor_parts.schema_name)

            assert descriptor_parts.type == "table"
            table = schema.by_name("table", descriptor_parts.name)

            contents = table.version().column(parameters.column_name)
            # Since the table is a Pyarrow table we need to produce some values.
            not_null_count = pc.count(contents, "only_valid").as_py()
            null_count = pc.count(contents, "only_null").as_py()
            distinct_count = len(set(contents.to_pylist()))
            sorted_contents = sorted(filter(lambda x: x is not None, contents.to_pylist()))
            min_value = sorted_contents[0]
            max_value = sorted_contents[-1]

            if contents.type == pa.uuid():
                # For UUIDs, we need to convert them to strings for the output.
                min_value = min_value.bytes
                max_value = max_value.bytes

            additional_values = {}
            additional_schema_fields = []
            if contents.type in (pa.string(), pa.utf8(), pa.binary()):
                max_length = pc.max(pc.binary_length(contents)).as_py()

                additional_values = {"max_string_length": max_length, "contains_unicode": contents.type == pa.utf8()}
                additional_schema_fields = [
                    pa.field("max_string_length", pa.uint64()),
                    pa.field("contains_unicode", pa.bool_()),
                ]

            result_table = pa.Table.from_pylist(
                [
                    {
                        "has_not_null": not_null_count > 0,
                        "has_null": null_count > 0,
                        "distinct_count": distinct_count,
                        "min": min_value,
                        "max": max_value,
                        **additional_values,
                    }
                ],
                schema=pa.schema(
                    [
                        pa.field("has_not_null", pa.bool_()),
                        pa.field("has_null", pa.bool_()),
                        pa.field("distinct_count", pa.uint64()),
                        pa.field("min", contents.type),
                        pa.field("max", contents.type),
                        *additional_schema_fields,
                    ]
                ),
            )
        return result_table

    def impl_do_get(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        ticket: flight.Ticket,
    ) -> flight.RecordBatchStream:
        ticket_data = flight_handling.decode_ticket_model(ticket, FlightTicketData)

        descriptor_parts = descriptor_unpack_(ticket_data.descriptor)
        with DatabaseLibraryContext() as library:
            database = library.by_name(descriptor_parts.catalog_name)
            schema = database.by_name(descriptor_parts.schema_name)

            if descriptor_parts.type == "table":
                table = schema.by_name("table", descriptor_parts.name)

                if descriptor_parts.name == "identity":
                    header_middleware = context.context.get_middleware("headers")
                    assert header_middleware
                    airport_client_session_id = header_middleware.client_headers.get("airport-client-session-id")

                    if airport_client_session_id is None or len(airport_client_session_id) == 0:
                        raise flight.FlightServerError("Missing airport-client-session-id header")

                    table_version = pa.Table.from_pylist(
                        [
                            {
                                "name": uuid_to_name(airport_client_session_id[0]),
                            }
                        ],
                        schema=pa.schema(
                            [
                                pa.field("name", pa.string()),
                            ],
                        ),
                    )
                else:
                    table_version = table.version()

                return flight.RecordBatchStream(table_version)
            else:
                raise flight.FlightServerError(f"Unsupported descriptor type: {descriptor_parts.type}")

    def action_flight_info(
        self,
        *,
        context: base_server.CallContext[auth.Account, auth.AccountToken],
        parameters: parameter_types.FlightInfo,
    ) -> flight.FlightInfo:
        with DatabaseLibraryContext() as library:
            descriptor_parts = descriptor_unpack_(parameters.descriptor)

            database = library.by_name(descriptor_parts.catalog_name)
            schema = database.by_name(descriptor_parts.schema_name)

            if descriptor_parts.type == "table":
                table = schema.by_name("table", descriptor_parts.name)

                version_id = None
                if parameters.at_unit is not None:
                    if parameters.at_unit == "VERSION":
                        assert parameters.at_value is not None

                        # Check if at_value is an integer but currently is a string.
                        if not re.match(r"^\d+$", parameters.at_value):
                            raise flight.FlightServerError(f"Invalid version: {parameters.at_value}")

                        version_id = int(parameters.at_value)
                    elif parameters.at_unit == "TIMESTAMP":
                        raise flight.FlightServerError("Timestamp not supported for table versioning")
                    else:
                        raise flight.FlightServerError(f"Unsupported at_unit: {parameters.at_unit}")

                return table.flight_info(
                    name=descriptor_parts.name,
                    catalog_name=descriptor_parts.catalog_name,
                    schema_name=descriptor_parts.schema_name,
                    version=version_id,
                )[0]
            else:
                raise flight.FlightServerError(f"Unsupported descriptor type: {descriptor_parts.type}")


@click.command()
@click.option(
    "--location",
    type=str,
    default="grpc://127.0.0.1:50333",
    help="The location where the server should listen.",
)
def run(location: str) -> None:
    log.info("Starting server", location=location)

    auth_manager = auth_manager_naive.AuthManagerNaive[auth.Account, auth.AccountToken](
        account_type=auth.Account,
        token_type=auth.AccountToken,
        allow_anonymous_access=True,
    )

    server = InMemoryArrowFlightServer(
        middleware={
            "headers": base_middleware.SaveHeadersMiddlewareFactory(),
            "auth": base_middleware.AuthManagerMiddlewareFactory(auth_manager=auth_manager),
        },
        location=location,
        auth_manager=auth_manager,
    )
    server.serve()
