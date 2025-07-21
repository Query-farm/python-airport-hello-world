import os
import pickle
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

import pyarrow as pa
import pyarrow.flight as flight
import query_farm_flight_server.flight_handling as flight_handling
import query_farm_flight_server.flight_inventory as flight_inventory

from .utils import CaseInsensitiveDict

# Since we are creating a new database, lets load it with a few example
# scalar functions.


def serialize_table_data(table: pa.Table) -> bytes:
    """
    Serialize the table data to a byte string.
    """
    assert isinstance(table, pa.Table)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def deserialize_table_data(data: bytes) -> pa.Table:
    """
    Deserialize the table data from a byte string.
    """
    assert isinstance(data, bytes)
    buffer = pa.BufferReader(data)
    ipc_stream = pa.ipc.open_stream(buffer)
    return ipc_stream.read_all()


@dataclass
class TableInfo:
    # To enable version history keep track of tables.
    table_versions: list[pa.Table] = field(default_factory=list)

    # the next row id to assign.
    row_id_counter: int = 0

    frozen: bool = False

    # This cannot be serailized but it convenient for testing.
    endpoint_generator: Callable[[Any], list[flight.FlightEndpoint]] | None = None

    def update_table(self, table: pa.Table) -> None:
        assert table is not None
        assert isinstance(table, pa.Table)
        self.table_versions.append(table)

    def version(self, version: int | None = None) -> pa.Table:
        """
        Get the version of the table.
        """
        assert len(self.table_versions) > 0
        if version is None:
            return self.table_versions[-1]

        assert version < len(self.table_versions)
        return self.table_versions[version]

    def flight_info(
        self,
        *,
        name: str,
        catalog_name: str,
        schema_name: str,
        version: int | None = None,
    ) -> tuple[flight.FlightInfo, flight_inventory.FlightSchemaMetadata]:
        """
        Often its necessary to create a FlightInfo object for the table,
        standardize doing that here.
        """
        metadata = flight_inventory.FlightSchemaMetadata(
            type="table",
            catalog=catalog_name,
            schema=schema_name,
            name=name,
            comment=None,
        )
        flight_info = flight.FlightInfo(
            self.version(version).schema,
            descriptor_pack_(catalog_name, schema_name, "table", name),
            [],
            -1,
            -1,
            app_metadata=metadata.serialize(),
        )
        return (flight_info, metadata)

    def serialize(self) -> dict[str, Any]:
        """
        Serialize the TableInfo to a dictionary.
        """
        return {
            "table_versions": [serialize_table_data(table) for table in self.table_versions],
            "row_id_counter": self.row_id_counter,
            "frozen": self.frozen,
        }

    def deserialize(self, data: dict[str, Any]) -> "TableInfo":
        """
        Deserialize the TableInfo from a dictionary.
        """
        self.table_versions = [deserialize_table_data(table) for table in data["table_versions"]]
        self.row_id_counter = data["row_id_counter"]
        self.endpoint_generator = None
        self.frozen = data["frozen"]
        return self


ObjectTypeName = Literal["table"]


@dataclass
class DescriptorParts:
    """
    The fields that are encoded in the flight descriptor.
    """

    catalog_name: str
    schema_name: str
    type: ObjectTypeName
    name: str


def descriptor_pack_(
    catalog_name: str,
    schema_name: str,
    type: ObjectTypeName,
    name: str,
) -> flight.FlightDescriptor:
    """
    Pack the descriptor into a FlightDescriptor.
    """
    return flight.FlightDescriptor.for_path(f"{catalog_name}/{schema_name}/{type}/{name}")


def descriptor_unpack_(descriptor: flight.FlightDescriptor) -> DescriptorParts:
    """
    Split the descriptor into its components.
    """
    assert descriptor.descriptor_type == flight.DescriptorType.PATH
    assert len(descriptor.path) == 1
    path = descriptor.path[0].decode("utf-8")
    parts = path.split("/")
    if len(parts) != 4:
        raise flight.FlightServerError(f"Invalid descriptor path: {path}")

    descriptor_type: ObjectTypeName
    if parts[2] == "table":
        descriptor_type = "table"
    else:
        raise flight.FlightServerError(f"Invalid descriptor type: {parts[2]}")

    return DescriptorParts(
        catalog_name=parts[0],
        schema_name=parts[1],
        type=descriptor_type,
        name=parts[3],
    )


@dataclass
class SchemaCollection:
    tables_by_name: CaseInsensitiveDict[TableInfo] = field(default_factory=CaseInsensitiveDict[TableInfo])

    def serialize(self) -> dict[str, Any]:
        return {
            "tables": {name: table.serialize() for name, table in self.tables_by_name.items()},
        }

    def deserialize(self, data: dict[str, Any]) -> "SchemaCollection":
        """
        Deserialize the schema collection from a dictionary.
        """
        self.tables_by_name = CaseInsensitiveDict[TableInfo](
            {name: TableInfo().deserialize(table) for name, table in data["tables"].items()}
        )
        return self

    def containers(
        self,
    ) -> list[CaseInsensitiveDict[TableInfo]]:
        return [self.tables_by_name]

    def by_name(self, type: Literal["table"], name: str) -> TableInfo:
        assert name is not None
        assert name != ""
        if type == "table":
            table = self.tables_by_name.get(name)
            if not table:
                raise flight.FlightServerError(f"Table {name} does not exist.")
            return table


@dataclass
class DatabaseContents:
    # Collection of schemas by name.
    schemas_by_name: CaseInsensitiveDict[SchemaCollection] = field(
        default_factory=CaseInsensitiveDict[SchemaCollection]
    )

    # The version of the database, updated on each schema change.
    version: int = 1

    def __post_init__(self) -> None:
        self.schemas_by_name["remote_data"] = remote_data_schema
        self.schemas_by_name["static"] = static_schema
        self.schemas_by_name["chat"] = chat_schema
        return

    def by_name(self, name: str) -> SchemaCollection:
        if name not in self.schemas_by_name:
            raise flight.FlightServerError(f"Schema {name} does not exist.")
        return self.schemas_by_name[name]

    def serialize(self) -> dict[str, Any]:
        return {
            "schemas": {name: schema.serialize() for name, schema in self.schemas_by_name.items()},
            "version": self.version,
        }

    def deserialize(self, data: dict[str, Any]) -> "DatabaseContents":
        """
        Deserialize the database contents from a dictionary.
        """
        self.schemas_by_name = CaseInsensitiveDict[SchemaCollection](
            {name: SchemaCollection().deserialize(schema) for name, schema in data["schemas"].items()}
        )
        self.schemas_by_name["static"] = static_schema
        self.schemas_by_name["remote_data"] = remote_data_schema
        self.schemas_by_name["chat"] = chat_schema

        self.version = data["version"]
        return self


@dataclass
class DatabaseLibrary:
    """
    The database library, which contains all of the databases, organized by token.
    """

    # Collection of databases by token.
    databases_by_name: CaseInsensitiveDict[DatabaseContents] = field(
        default_factory=CaseInsensitiveDict[DatabaseContents]
    )

    def __post_init__(self) -> None:
        self.databases_by_name["hello"] = DatabaseContents()
        return

    def by_name(self, name: str) -> DatabaseContents:
        if name not in self.databases_by_name:
            raise flight.FlightServerError(f"Database {name} does not exist.")
        return self.databases_by_name[name]

    def serialize(self) -> dict[str, Any]:
        return {
            "databases": {name: db.serialize() for name, db in self.databases_by_name.items()},
        }

    def deserialize(self, data: dict[str, Any]) -> None:
        """
        Deserialize the database library from a dictionary.
        """
        self.databases_by_name = CaseInsensitiveDict[DatabaseContents](
            {name: DatabaseContents().deserialize(db) for name, db in data["databases"].items()}
        )

    @staticmethod
    def filename_for_token(token: str) -> str:
        """
        Get the filename for the database library for a given token.
        """
        assert token is not None
        assert token != ""
        return f"database_library_{token}.pkl"

    @staticmethod
    def reset(token: str) -> None:
        """
        Reset the database library for a given token.
        This will delete the file associated with the token.
        """
        file_path = DatabaseLibrary.filename_for_token(token)
        if os.path.isfile(file_path):
            os.remove(file_path)

    @staticmethod
    def read_from_file(token: str) -> "DatabaseLibrary":
        """
        Read the database library from a file.
        If the file does not exist, return an empty database library.
        """
        library = DatabaseLibrary()

        file_path = DatabaseLibrary.filename_for_token(token)

        if not os.path.isfile(file_path):
            # File doesn't exist — return empty instance
            return library

        try:
            with open(file_path, "rb") as f:
                # use pickle
                data = pickle.load(f)
                library.deserialize(data)
        except Exception as e:
            raise RuntimeError(f"Failed to read database library from {file_path}: {e}") from e

        return library

    def write_to_file(self, token: str) -> None:
        """
        Write the database library to a temp file, then atomically rename to the destination.
        """
        file_path = DatabaseLibrary.filename_for_token(token)

        data = self.serialize()
        # use pickle
        dir_name = os.path.dirname(file_path) or "."

        with tempfile.NamedTemporaryFile("wb", dir=dir_name, delete=False) as tmp_file:
            pickle.dump(data, tmp_file)
        os.replace(tmp_file.name, file_path)


DATABASE_FILENAME = "foobar"


class DatabaseLibraryContext:
    def __init__(self, readonly: bool = False) -> None:
        self.readonly = readonly

    def __enter__(self) -> DatabaseLibrary:
        self.db = DatabaseLibrary.read_from_file(DATABASE_FILENAME)
        return self.db

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type:
            print(f"An error occurred: {exc_val}")
            # Optionally return True to suppress the exception
            return
        if not self.readonly:
            self.db.write_to_file(DATABASE_FILENAME)


def yellow_taxi_endpoint_generator(ticket_data: Any) -> list[flight.FlightEndpoint]:
    """
    Generate a list of FlightEndpoint objects for the NYC Yellow Taxi dataset.
    """
    files = [
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-02.parquet",
        #        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-03.parquet",
        #        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-04.parquet",
        #        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-05.parquet",
    ]
    return [
        flight_handling.endpoint(
            ticket_data=ticket_data,
            locations=[
                flight_handling.dict_to_msgpack_duckdb_call_data_uri(
                    {
                        "function_name": "read_parquet",
                        # So arguments could be a record batch.
                        "data": flight_handling.serialize_arrow_ipc_table(
                            pa.Table.from_pylist(
                                [
                                    {
                                        "arg_0": files,
                                        "hive_partitioning": False,
                                        "union_by_name": True,
                                    }
                                ],
                            )
                        ),
                    }
                )
            ],
        )
    ]


remote_data_schema = SchemaCollection(
    tables_by_name=CaseInsensitiveDict(
        {
            "nyc_yellow_taxi": TableInfo(
                table_versions=[
                    pa.schema(
                        [
                            pa.field("VendorID", pa.int32()),
                            pa.field("tpep_pickup_datetime", pa.timestamp("us")),
                            pa.field("tpep_dropoff_datetime", pa.timestamp("us")),
                            pa.field("passenger_count", pa.int64()),
                            pa.field("trip_distance", pa.float64()),
                            pa.field("RatecodeID", pa.int64()),
                            pa.field("store_and_fwd_flag", pa.string()),
                            pa.field("PULocationID", pa.int32()),
                            pa.field("DOLocationID", pa.int32()),
                            pa.field("payment_type", pa.int64()),
                            pa.field("fare_amount", pa.float64()),
                            pa.field("extra", pa.float64()),
                            pa.field("mta_tax", pa.float64()),
                            pa.field("tip_amount", pa.float64()),
                            pa.field("tolls_amount", pa.float64()),
                            pa.field("improvement_surcharge", pa.float64()),
                            pa.field("total_amount", pa.float64()),
                            pa.field("congestion_surcharge", pa.float64()),
                            pa.field("Airport_fee", pa.float64()),
                            pa.field("cbd_congestion_fee", pa.float64()),
                        ]
                    ).empty_table()
                ],
                row_id_counter=0,
                endpoint_generator=yellow_taxi_endpoint_generator,
            ),
        }
    ),
)

chat_identity_schema = pa.schema(
    [
        pa.field("name", pa.string()),
    ]
)

chat_messages_schema = pa.schema(
    [
        pa.field("rowid", pa.int64(), metadata={"is_rowid": "1"}),
        pa.field("id", pa.int64()),
        pa.field("sender", pa.string()),
        pa.field("message", pa.string()),
        pa.field("timestamp", pa.timestamp("ms")),
        pa.field("channel", pa.string()),
    ]
)


chat_schema = SchemaCollection(
    tables_by_name=CaseInsensitiveDict(
        {
            "messages": TableInfo(
                table_versions=[chat_messages_schema.empty_table()],
                row_id_counter=0,
            ),
            "identity": TableInfo(
                frozen=True,
                table_versions=[
                    chat_identity_schema.empty_table(),
                ],
                row_id_counter=0,
            ),
        }
    ),
)

static_schema = SchemaCollection(
    tables_by_name=CaseInsensitiveDict(
        {
            "people": TableInfo(
                frozen=True,
                table_versions=[
                    pa.Table.from_pylist(
                        [
                            {"language": "English", "first_name": "James", "last_name": "Smith"},
                            {"language": "Spanish", "first_name": "Carlos", "last_name": "García"},
                            {"language": "French", "first_name": "Émile", "last_name": "Dubois"},
                            {"language": "German", "first_name": "Lukas", "last_name": "Müller"},
                            {"language": "Italian", "first_name": "Giulia", "last_name": "Rossi"},
                            {"language": "Portuguese", "first_name": "João", "last_name": "Silva"},
                            {"language": "Russian", "first_name": "Анастасия", "last_name": "Иванова"},
                            {"language": "Japanese", "first_name": "はるき", "last_name": "田中"},
                            {"language": "Chinese (Mandarin)", "first_name": "莲", "last_name": "王"},
                            {"language": "Korean", "first_name": "지수", "last_name": "김"},
                            {"language": "Arabic", "first_name": "عمر", "last_name": "حسن"},
                            {"language": "Hindi", "first_name": "आरव", "last_name": "शर्मा"},
                            {"language": "Turkish", "first_name": "Elif", "last_name": "Yılmaz"},
                            {"language": "Swahili", "first_name": "Amani", "last_name": "Juma"},
                            {"language": "Greek", "first_name": "Νίκος", "last_name": "Παπαδόπουλος"},
                            {"language": "Hebrew", "first_name": "נועה", "last_name": "כהן"},
                            {"language": "Thai", "first_name": "นิรันดร์", "last_name": "สุขุม"},
                            {"language": "Vietnamese", "first_name": "An", "last_name": "Nguyễn"},
                            {"language": "Dutch", "first_name": "Sven", "last_name": "De Vries"},
                            {"language": "Swedish", "first_name": "Elsa", "last_name": "Andersson"},
                            {"language": "Finnish", "first_name": "Mikko", "last_name": "Korhonen"},
                            {"language": "Polish", "first_name": "Kasia", "last_name": "Kowalska"},
                            {"language": "Indonesian", "first_name": "Putri", "last_name": "Santoso"},
                            {"language": "Filipino (Tagalog)", "first_name": "Bayani", "last_name": "Reyes"},
                            {"language": "Malay", "first_name": "Azlan", "last_name": "Ismail"},
                            {"language": "Hungarian", "first_name": "Zsófia", "last_name": "Nagy"},
                        ],
                        schema=pa.schema(
                            [
                                pa.field("language", pa.string()),
                                pa.field("first_name", pa.utf8()),
                                pa.field("last_name", pa.utf8()),
                            ],
                            #                           metadata={"can_produce_statistics": "1"},
                        ),
                    ),
                ],
                row_id_counter=100,
            ),
            "greetings": TableInfo(
                frozen=True,
                table_versions=[
                    pa.Table.from_pylist(
                        [
                            {"language": "English", "greeting": "Hello"},
                            {"language": "Spanish", "greeting": "Hola"},
                            {"language": "French", "greeting": "Bonjour"},
                            {"language": "German", "greeting": "Hallo"},
                            {"language": "Italian", "greeting": "Ciao"},
                            {"language": "Portuguese", "greeting": "Olá"},
                            {"language": "Russian", "greeting": "Здравствуйте"},
                            {"language": "Japanese", "greeting": "こんにちは"},
                            {"language": "Chinese (Mandarin)", "greeting": "你好"},
                            {"language": "Korean", "greeting": "안녕하세요"},
                            {"language": "Arabic", "greeting": "مرحبا"},
                            {"language": "Hindi", "greeting": "नमस्ते"},
                            {"language": "Turkish", "greeting": "Merhaba"},
                            {"language": "Swahili", "greeting": "Jambo"},
                            {"language": "Greek", "greeting": "Γειά σου"},
                            {"language": "Hebrew", "greeting": "שלום"},
                            {"language": "Thai", "greeting": "สวัสดี"},
                            {"language": "Vietnamese", "greeting": "Xin chào"},
                            {"language": "Dutch", "greeting": "Hallo"},
                            {"language": "Swedish", "greeting": "Hej"},
                            {"language": "Finnish", "greeting": "Hei"},
                            {"language": "Polish", "greeting": "Cześć"},
                            {"language": "Indonesian", "greeting": "Halo"},
                            {"language": "Filipino (Tagalog)", "greeting": "Kamusta"},
                            {"language": "Malay", "greeting": "Hai"},
                            {"language": "Hungarian", "greeting": "Helló"},
                            {"language": "Hungarian (informal)", "greeting": "Szia"},
                        ],
                        schema=pa.schema(
                            [
                                pa.field("greeting", pa.utf8()),
                                pa.field("language", pa.string()),
                            ],
                            metadata={"can_produce_statistics": "1"},
                        ),
                    ),
                ],
                row_id_counter=100,
            ),
        }
    ),
)
