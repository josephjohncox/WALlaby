import inspect
import os
import time

import uvicorn


def _load_server_func():
    try:
        import fakesnow
    except Exception:
        return None

    server_fn = getattr(fakesnow, "server", None)
    if server_fn is not None:
        return server_fn

    try:
        from fakesnow import server as server_mod  # type: ignore
    except Exception:
        return None

    return getattr(server_mod, "server", None)


def _load_asgi_app():
    candidates = [
        "fakesnow.server",
        "fakesnow.app",
        "fakesnow.api",
        "fakesnow.http",
        "fakesnow.main",
    ]
    for name in candidates:
        try:
            module = __import__(name, fromlist=["app"])
        except Exception:
            continue
        app = getattr(module, "app", None)
        if app is not None:
            return app
        create_app = getattr(module, "create_app", None)
        if create_app is not None:
            return create_app()
    return None


def main():
    host = os.getenv("FAKESNOW_HOST", "0.0.0.0")
    port = int(os.getenv("FAKESNOW_PORT", "8000"))

    app = _load_asgi_app()
    if app is None:
        raise RuntimeError("Unable to locate fakesnow ASGI app")

    print(f"fakesnow uvicorn starting on {host}:{port}", flush=True)
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
