import sys
from pathlib import Path

# Añadir el directorio generado al path para resolver las importaciones directas de protoc
_current_dir = str(Path(__file__).resolve().parent)
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)

from . import inventory_pb2
from . import inventory_pb2_grpc

__all__ = ["inventory_pb2", "inventory_pb2_grpc"]

