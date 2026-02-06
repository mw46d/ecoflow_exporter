# I call this Android for now because the proto messages were extracted
# from the Android client. I don't know yet, which devices will actually
# publish these messages?!

import json
import logging
import time

from paho.mqtt.client import MQTTMessage

from devices.common import EcoflowCommon
from devices.proto import android_pb2 
from typing import Any

_LOGGER = logging.getLogger(__name__)

class Android(EcoflowCommon):
    """Android device implementation using protobuf decoding."""

    def __init__(self):
        pass


    def get_payload(self, raw_data: bytes):
        return json.dumps(self._prepare_data(raw_data))


    def _prepare_data(self, raw_data: bytes) -> dict[str, Any]:
        """Prepare Android data by decoding protobuf and flattening fields."""
        flat_dict: dict[str, Any] | None = None
        decoded_data: dict[str, Any] | None = None
        try:
            header_info = self._decode_header_message(raw_data)
            if header_info is None:
                return {}

            pdata = self._extract_payload_data(header_info.get("header_obj"))
            if not pdata:
                return {}

            decoded_pdata = self._perform_xor_decode(pdata, header_info)
            decoded_data = self._decode_message_by_type(decoded_pdata, header_info)
            if not decoded_data:
                return {}

            flat_dict = self._flatten_dict(decoded_data)
        except Exception as e:
            _LOGGER.debug(f"Android Data processing failed: {e}")
            return super()._prepare_data(raw_data)

        return {
            "params": flat_dict or {},
            "all_fields": decoded_data or {},
        }


    def _extract_payload_data(self, header_obj: Any) -> bytes | None:
        """Extract payload bytes from header."""
        try:
            pdata = getattr(header_obj, "pdata", b"")
            return pdata if pdata else None
        except Exception as e:
            _LOGGER.debug("Android Failed to extract payload data: %s", e)
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
        - cmdFunc=254, cmdId=23: DevRequest ??
        - cmdFunc=254, cmdId=25: SafetyParamSet ??
        - cmdFunc=32, cmdId=177: BpCloudHeartbeatReport ??
        - cmdFunc=240, cmdId=5:  EDevSysReport ??
        - cmdFunc=240, cmdId=36: EDevEnergyRemoteListSync but enum DevAddr ??
        """
        cmd_func = header_info.get("cmdFunc", 0)
        cmd_id = header_info.get("cmdId", 0)

        try:
            if cmd_func == 254 and cmd_id == 21:
                msg_display_upload = android_pb2.DisplayPropertyUpload()
                msg_display_upload.ParseFromString(pdata)
                result = self._protobuf_to_dict(msg_display_upload)
                return self._extract_statistics(result)

            elif cmd_func == 254 and cmd_id == 22:
                msg_runtime_upload = android_pb2.RuntimePropertyUpload()
                msg_runtime_upload.ParseFromString(pdata)
                return self._protobuf_to_dict(msg_runtime_upload)
            elif cmd_func == 254 and cmd_id == 23:
                msg_dev_request = android_pb2.DevRequest()
                msg_dev_request.ParseFromString(pdata)
                return self._protobuf_to_dict(msg_dev_request)
            elif cmd_func == 254 and cmd_id == 25:
                msg_safety_param_set = android_pb2.SafetyParamSet()
                msg_safety_param_set.ParseFromString(pdata)
                return self._protobuf_to_dict(msg_safety_param_set)
            elif cmd_func == 32 and cmd_id == 177:
                msg_bp_cloud_heartbeat = android_pb2.BpCloudHeartbeatReport()
                msg_bp_cloud_heartbeat.ParseFromString(pdata)
                return self._protobuf_to_dict(msg_bp_cloud_heartbeat)
            elif cmd_func == 240 and cmd_id == 5:
                msg_edev_sys = android_pb2.EDevSysReport()
                msg_edev_sys.ParseFromString(pdata)
                return self._protobuf_to_dict(msg_edev_sys)
            elif cmd_func == 240 and cmd_id == 36:
                msg_edev_energy_remote = android_pb2.EDevEnergyRemoteListSync()
                msg_edev_energy_remote.ParseFromString(pdata)
                return self._protobuf_to_dict(msg_edev_energy_remote)
            else:
                _LOGGER.info(f"MW _decode_message_by_type cmd_func= {cmd_func} cmd_id= {cmd_id} len(pdata)= {len(pdata)}")
                with open(f"/tmp/mw_{cmd_func}:{cmd_id}_{round(time.time() * 1000)}.msg", 'wb') as f:
                    f.write(pdata)
                return {}
        except Exception as e:
            _LOGGER.warn(f"Message decode error for cmdFunc={cmd_func}, cmdId={cmd_id}: {e}")

        return {}
