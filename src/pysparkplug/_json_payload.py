"""Module providing JSON-based payload encoding for Sparkplug B"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional, cast

from pysparkplug._datatype import DataType
from pysparkplug._metric import Metric
from pysparkplug._types import Self

def metric_to_json(metric: Metric, *, include_dtype: bool = False) -> Dict[str, Any]:
    """Convert a Metric to a JSON-serializable dictionary."""
    result: Dict[str, Any] = {}
    
    if metric.name is not None:
        result["name"] = metric.name
    if metric.alias is not None:
        result["alias"] = metric.alias
    if metric.timestamp is not None:
        result["timestamp"] = metric.timestamp
    if metric.datatype != DataType.UNKNOWN:
        result["datatype"] = metric.datatype.value
    if metric.is_historical:
        result["is_historical"] = True
    if metric.is_transient:
        result["is_transient"] = True
    if metric.is_null:
        result["is_null"] = True
    if metric.metadata is not None:
        result["metadata"] = metric.metadata
    if metric.properties is not None:
        result["properties"] = metric.properties
        
    # Handle value based on datatype
    if not metric.is_null and metric.value is not None:
        if metric.datatype in (DataType.BOOLEAN, DataType.INT8, DataType.INT16, 
                             DataType.INT32, DataType.INT64, DataType.UINT8, 
                             DataType.UINT16, DataType.UINT32, DataType.UINT64,
                             DataType.FLOAT, DataType.DOUBLE, DataType.STRING):
            result["value"] = metric.value
        elif metric.datatype == DataType.BYTES:
            result["value"] = list(metric.value)
        elif metric.datatype == DataType.DATASET:
            result["value"] = {
                "num_of_columns": metric.value.num_of_columns,
                "columns": [col.name for col in metric.value.columns],
                "types": [col.type.value for col in metric.value.columns],
                "rows": [[cell.value for cell in row.cells] for row in metric.value.rows]
            }
        elif metric.datatype == DataType.TEMPLATE:
            result["value"] = {
                "version": metric.value.version,
                "template_ref": metric.value.template_ref,
                "is_definition": metric.value.is_definition,
                "metrics": [metric_to_json(m, include_dtype=include_dtype) for m in metric.value.metrics],
                "parameters": [
                    {
                        "name": p.name,
                        "type": p.type.value,
                        "value": p.value
                    } for p in metric.value.parameters
                ]
            }
            
    return result

def metric_from_json(data: Dict[str, Any]) -> Metric:
    """Create a Metric from a JSON dictionary."""
    kwargs: Dict[str, Any] = {}
    
    if "name" in data:
        kwargs["name"] = data["name"]
    if "alias" in data:
        kwargs["alias"] = data["alias"]
    if "timestamp" in data:
        kwargs["timestamp"] = data["timestamp"]
    if "datatype" in data:
        kwargs["datatype"] = DataType(data["datatype"])
    if data.get("is_historical"):
        kwargs["is_historical"] = True
    if data.get("is_transient"):
        kwargs["is_transient"] = True
    if data.get("is_null"):
        kwargs["is_null"] = True
    if "metadata" in data:
        kwargs["metadata"] = data["metadata"]
    if "properties" in data:
        kwargs["properties"] = data["properties"]
        
    # Handle value based on datatype
    if "value" in data and not data.get("is_null"):
        if kwargs.get("datatype") in (DataType.BOOLEAN, DataType.INT8, DataType.INT16, 
                                    DataType.INT32, DataType.INT64, DataType.UINT8, 
                                    DataType.UINT16, DataType.UINT32, DataType.UINT64,
                                    DataType.FLOAT, DataType.DOUBLE, DataType.STRING):
            kwargs["value"] = data["value"]
        elif kwargs.get("datatype") == DataType.BYTES:
            kwargs["value"] = bytes(data["value"])
        elif kwargs.get("datatype") == DataType.DATASET:
            from pysparkplug._protobuf import DataSet, DataSetValue, Row
            dataset = DataSet()
            dataset.num_of_columns = data["value"]["num_of_columns"]
            for col_name, col_type in zip(data["value"]["columns"], data["value"]["types"]):
                col = dataset.columns.add()
                col.name = col_name
                col.type = col_type
            for row_data in data["value"]["rows"]:
                row = Row()
                for cell_value in row_data:
                    cell = row.cells.add()
                    cell.value = cell_value
                dataset.rows.append(row)
            kwargs["value"] = dataset
        elif kwargs.get("datatype") == DataType.TEMPLATE:
            from pysparkplug._protobuf import Template, Parameter
            template = Template()
            template.version = data["value"]["version"]
            template.template_ref = data["value"]["template_ref"]
            template.is_definition = data["value"]["is_definition"]
            for metric_data in data["value"]["metrics"]:
                template.metrics.append(metric_from_json(metric_data).to_pb())
            for param_data in data["value"]["parameters"]:
                param = Parameter()
                param.name = param_data["name"]
                param.type = param_data["type"]
                param.value = param_data["value"]
                template.parameters.append(param)
            kwargs["value"] = template
            
    return Metric(**kwargs)

def payload_to_json(payload: Any, *, include_dtypes: bool = False) -> bytes:
    """Convert a payload to JSON bytes."""
    data: Dict[str, Any] = {
        "timestamp": payload.timestamp
    }
    
    if hasattr(payload, "seq"):
        data["seq"] = payload.seq
        
    if hasattr(payload, "metrics"):
        data["metrics"] = [metric_to_json(m, include_dtype=include_dtypes) for m in payload.metrics]
    elif hasattr(payload, "bd_seq_metric"):
        data["bd_seq_metric"] = metric_to_json(payload.bd_seq_metric, include_dtype=include_dtypes)
    elif hasattr(payload, "online"):
        data["online"] = payload.online
        
    return json.dumps(data).encode()

def payload_from_json(cls: type[Self], raw: bytes, *, birth: Optional[Any] = None) -> Self:
    """Create a payload from JSON bytes."""
    data = json.loads(raw.decode())
    
    kwargs: Dict[str, Any] = {
        "timestamp": data["timestamp"]
    }
    
    if "seq" in data:
        kwargs["seq"] = data["seq"]
        
    if "metrics" in data:
        metrics = [metric_from_json(m) for m in data["metrics"]]
        if birth is not None:
            for metric in metrics:
                if not metric.name and metric.alias is not None:
                    metric.name = birth.get_name(metric.alias)
                if metric.datatype == DataType.UNKNOWN:
                    metric.datatype = birth.get_dtype(metric.name)
        kwargs["metrics"] = tuple(metrics)
    elif "bd_seq_metric" in data:
        kwargs["bd_seq_metric"] = metric_from_json(data["bd_seq_metric"])
    elif "online" in data:
        kwargs["online"] = data["online"]
        
    return cls(**kwargs) 