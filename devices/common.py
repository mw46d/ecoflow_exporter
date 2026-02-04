import json
import logging

from paho.mqtt.client import MQTTMessage
from devices.proto import ef_river3_pb2
from typing import Any

_LOGGER = logging.getLogger(__name__)


class EcoflowCommon():

    BMS_HEARTBEAT_COMMANDS: set[tuple[int, int]] = {
        (3, 1),
        (3, 2),
        (3, 30),
        (3, 50),
        (32, 1),
        (32, 3),
        (32, 50),
        (32, 51),
        (32, 52),
        (254, 24),
        (254, 25),
        (254, 26),
        (254, 27),
        (254, 28),
        (254, 29),
        (254, 30),
    }

    def __init__(self):
        pass


    def get_header(self, raw_data: bytes):
        return json.dumps(self._decode_header_message(raw_data))


    def get_payload(self, raw_data: bytes):
        return json.dumps(self._prepare_data(raw_data))


    def _decode_header_message(self, raw_data: bytes) -> dict[str, Any] | None:
        """Decode HeaderMessage and extract header info."""
        try:
            header_msg = ef_river3_pb2.River3HeaderMessage()
            header_msg.ParseFromString(raw_data)
        except Exception as e:
            _LOGGER.debug("[River3] Failed to parse header message: %s", e)
            return None

        if not header_msg.header:
            return None

        header = header_msg.header[0]
        return {
            "src": getattr(header, "src", 0),
            "dest": getattr(header, "dest", 0),
            "dSrc": getattr(header, "d_src", 0),
            "dDest": getattr(header, "d_dest", 0),
            "encType": getattr(header, "enc_type", 0),
            "checkType": getattr(header, "check_type", 0),
            "cmdFunc": getattr(header, "cmd_func", 0),
            "cmdId": getattr(header, "cmd_id", 0),
            "dataLen": getattr(header, "data_len", 0),
            "needAck": getattr(header, "need_ack", 0),
            "seq": getattr(header, "seq", 0),
            "productId": getattr(header, "product_id", 0),
            "version": getattr(header, "version", 0),
            "payloadVer": getattr(header, "payload_ver", 0),
            "header_obj": header,
        }

    def _extract_payload_data(self, header_obj: Any) -> bytes | None:
        """Extract payload bytes from header."""
        try:
            pdata = getattr(header_obj, "pdata", b"")
            return pdata if pdata else None
        except Exception as e:
            _LOGGER.debug("[River3] Failed to extract payload data: %s", e)
            return None

    def _perform_xor_decode(self, pdata: bytes, header_info: dict[str, Any]) -> bytes:
        """Perform XOR decoding if required by header info."""
        enc_type = header_info.get("encType", 0)
        src = header_info.get("src", 0)
        seq = header_info.get("seq", 0)
        if enc_type == 1 and src != 32:
            return self._xor_decode_pdata(pdata, seq)
        return pdata

    def _xor_decode_pdata(self, pdata: bytes, seq: int) -> bytes:
        """Apply XOR over payload with sequence value."""
        if not pdata:
            return b""

        decoded_payload = bytearray()
        for byte_val in pdata:
            decoded_payload.append((byte_val ^ seq) & 0xFF)

        return bytes(decoded_payload)

    def _decode_message_by_type(self, pdata: bytes, header_info: dict[str, Any]) -> dict[str, Any]:
        """Decode protobuf message based on cmdFunc/cmdId.
        - cmdFunc=254, cmdId=21: DisplayPropertyUpload
        - cmdFunc=254, cmdId=22: RuntimePropertyUpload
        - cmdFunc=254, cmdId=17: Set command
        - cmdFunc=254, cmdId=18: Set reply
        """
        cmd_func = header_info.get("cmdFunc", 0)
        cmd_id = header_info.get("cmdId", 0)

        try:
            if cmd_func == 254 and cmd_id == 21:
                msg_display_upload = ef_river3_pb2.River3DisplayPropertyUpload()
                msg_display_upload.ParseFromString(pdata)
                result = self._protobuf_to_dict(msg_display_upload)
                return self._extract_statistics(result)

            elif cmd_func == 254 and cmd_id == 22:
                msg_runtime_upload = ef_river3_pb2.River3RuntimePropertyUpload()
                msg_runtime_upload.ParseFromString(pdata)
                return self._protobuf_to_dict(msg_runtime_upload)

            elif cmd_func == 254 and cmd_id == 17:
                try:
                    msg_set_command = ef_river3_pb2.River3SetCommand()
                    msg_set_command.ParseFromString(pdata)
                    return self._protobuf_to_dict(msg_set_command)
                except Exception as e:
                    _LOGGER.debug("Failed to decode as River3SetCommand: %s", e)
                    return {}

            elif cmd_func == 254 and cmd_id == 18:
                try:
                    msg_set_reply = ef_river3_pb2.River3SetReply()
                    msg_set_reply.ParseFromString(pdata)
                    result = self._protobuf_to_dict(msg_set_reply)
                    return result if result.get("config_ok", False) else {}
                except Exception as e:
                    _LOGGER.debug(f"Failed to decode as setReply_dp3: {e}")
                    return {}

            elif cmd_func == 32 and cmd_id == 2:
                try:
                    msg_cms_heartbeat = ef_river3_pb2.River3CMSHeartBeatReport()
                    msg_cms_heartbeat.ParseFromString(pdata)
                    return self._protobuf_to_dict(msg_cms_heartbeat)
                except Exception as e:
                    _LOGGER.debug(f"Failed to decode as cmdFunc32_cmdId2_Report: {e}")
                    return {}

            elif self._is_bms_heartbeat(cmd_func, cmd_id):
                try:
                    msg_bms_heartbeat = ef_river3_pb2.River3BMSHeartBeatReport()
                    msg_bms_heartbeat.ParseFromString(pdata)
                    return self._protobuf_to_dict(msg_bms_heartbeat)
                except Exception as e:
                    _LOGGER.debug(f"Failed to decode as BMSHeartBeatReport (cmdFunc={cmd_func}, cmdId={cmd_id}): {e}")
                    return {}

            # Unknown message type - try BMSHeartBeatReport as fallback
            try:
                msg_bms_heartbeat = ef_river3_pb2.River3BMSHeartBeatReport()
                msg_bms_heartbeat.ParseFromString(pdata)
                result = self._protobuf_to_dict(msg_bms_heartbeat)
                if "cycles" in result or "accu_chg_energy" in result or "accu_dsg_energy" in result:
                    return result
            except Exception as e:
                _LOGGER.debug("Failed to decode as fallback BMSHeartBeatReport: %s", e)

            return {}
        except Exception as e:
            _LOGGER.debug(f"Message decode error for cmdFunc={cmd_func}, cmdId={cmd_id}: {e}")
            return {}

    def _is_bms_heartbeat(self, cmd_func: int, cmd_id: int) -> bool:
        """Return True if the pair maps to a BMSHeartBeatReport message."""
        return (cmd_func, cmd_id) in BMS_HEARTBEAT_COMMANDS

    def _flatten_dict(self, d: dict, parent_key: str = "", sep: str = "_") -> dict:
        """Flatten nested dict with underscore separator."""
        items: list[tuple[str, Any]] = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def _protobuf_to_dict(self, protobuf_obj: Any) -> dict[str, Any]:
        """Convert protobuf message to dictionary."""
        try:
            from google.protobuf.json_format import MessageToDict

            return MessageToDict(protobuf_obj, preserving_proto_field_name=True)
        except ImportError:
            return self._manual_protobuf_to_dict(protobuf_obj)

    def _extract_statistics(self, data: dict[str, Any]) -> dict[str, Any]:
        """Extract statistics from display_statistics_sum into flat fields."""
        stats_sum = data.get("display_statistics_sum", {})
        list_info = stats_sum.get("list_info", [])

        if not list_info:
            return data

        for item in list_info:
            stat_obj = item.get("statistics_object") or item.get("statisticsObject")
            stat_content = item.get("statistics_content") or item.get("statisticsContent")

            if stat_obj is not None and stat_content is not None:
                if isinstance(stat_obj, str) and stat_obj.startswith("STATISTICS_OBJECT_"):
                    field_name = stat_obj.replace("STATISTICS_OBJECT_", "").lower()
                    data[field_name] = stat_content
                elif isinstance(stat_obj, int):
                    try:
                        enum_name = ef_river3_pb2.River3StatisticsObject.Name(stat_obj)
                        if enum_name.startswith("STATISTICS_OBJECT_"):
                            field_name = enum_name.replace("STATISTICS_OBJECT_", "").lower()
                            data[field_name] = stat_content
                    except ValueError as e:
                        _LOGGER.debug(
                            "Failed to get enum name for statistics object %s: %s",
                            stat_obj,
                            e,
                        )

        return data

    def _manual_protobuf_to_dict(self, protobuf_obj: Any) -> dict[str, Any]:
        """Convert protobuf object to dict manually (fallback)."""
        result: dict[str, Any] = {}
        for field, value in protobuf_obj.ListFields():
            if field.label == field.LABEL_REPEATED:
                result[field.name] = list(value)
            elif hasattr(value, "ListFields"):  # nested message
                result[field.name] = self._manual_protobuf_to_dict(value)
            else:
                result[field.name] = value
        return result
