"""One Feishu message HTTP operation. SDK token acquisition is reused separately."""
from copy import copy
import time


def send_request_once(config, request, timeout=55):
    from lark_oapi.core import JSON
    from lark_oapi.core.const import APPLICATION_JSON, CONTENT_TYPE
    from lark_oapi.core.http.transport import _build_header, _build_url
    from lark_oapi.core.model import RequestOption
    from lark_oapi.core.token import verify
    import requests

    started = time.monotonic()
    config = copy(config)
    config.timeout = timeout
    option = RequestOption()
    verify(config, request, option)
    remaining = timeout - (time.monotonic() - started)
    if remaining <= 0:
        return {"outcome": "failed", "error": "token_deadline_before_message_dispatch"}
    option.headers[CONTENT_TYPE] = f"{APPLICATION_JSON}; charset=utf-8"
    headers = _build_header(request, option, config)
    url = _build_url(config.domain, request.uri, request.paths)
    # No process-global SDK/request patches: ordinary sends keep their own policy.
    with requests.Session() as session:
        session.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))
        session.mount("http://", requests.adapters.HTTPAdapter(max_retries=0))
        response = session.request(request.http_method.name, url, headers=headers,
            params=request.queries, data=JSON.marshal(request.body).encode("utf-8"),
            allow_redirects=False, timeout=(min(10, remaining), remaining))
    if 300 <= response.status_code < 400:
        return {"outcome": "unknown", "error": "redirect_refused"}
    payload = response.json()
    message_id = (payload.get("data") or {}).get("message_id")
    if response.status_code == 200 and payload.get("code") == 0 and isinstance(message_id, str) and message_id:
        return {"outcome": "sent", "message_id": message_id}
    # Only explicit validation/auth/client refusals are known not to have succeeded.
    rejected = response.status_code in (400, 401, 403, 404, 413, 422)
    return {"outcome": "failed" if rejected else "unknown", "error": "feishu_code_" + str(payload.get("code"))}
