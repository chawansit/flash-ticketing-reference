"""Shared, offline release/environment contract (ADR0652). No execution authority."""
import ast
import hashlib
import json
import os
import re
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit

BINDINGS = {"database_url": "DATABASE_URL", "redis_url": "REDIS_URL",
            "kafka_bootstrap": "KAFKA_BOOTSTRAP", "jwt_secret": "JWT_SECRET",
            "webhook_secret": "WEBHOOK_SECRET"}
STAGES = {"development", "staging", "production"}
GUARDRAILS = {"backends": 24, "growth_bytes": 4 * 2**30, "reserve_bytes": 16 * 2**30,
              "whole_seconds": 1200, "pause_seconds": 600, "recovery_seconds": 300,
              "helper_cpu": "0.1", "helper_memory_bytes": 128 * 2**20, "helper_pids": 32,
              "helper_tmpfs_bytes": 2**20, "generator_cpu": "0.75", "generator_memory_bytes": 512 * 2**20}
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
NAME = re.compile(r"[a-z][a-z0-9-]{0,62}\Z")
ENV_NAME = re.compile(r"[A-Z][A-Z0-9_]{0,95}\Z")


def require(ok, code):
    if not ok:
        raise ValueError("CONTRACT_" + code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def seal(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def source_hash(data):
    return hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()


def keys(value, names, code):
    require(type(value) is dict and set(value) == set(names.split()), code)


def load(path):
    raw = Path(path).read_bytes()
    require(len(raw) <= 2**20, "DOCUMENT_TOO_LARGE")

    def pairs(items):
        out = {}
        for key, value in items:
            require(key not in out, "DUPLICATE_KEY")
            out[key] = value
        return out

    def bad_number(_):
        raise ValueError("CONTRACT_NONFINITE_NUMBER")

    return json.loads(raw, object_pairs_hook=pairs, parse_constant=bad_number)


def settings_layout(config_path=None):
    """Evaluate only existing Settings default expressions, with no process environment."""
    path = Path(config_path) if config_path else Path(__file__).with_name("config.py")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Settings")
    empty = SimpleNamespace(getenv=lambda _key, default=None: default, environ={})
    result = {}
    for node in cls.body:
        if not isinstance(node, ast.AnnAssign) or node.value is None:
            continue
        refs = [n.args[0].value for n in ast.walk(node.value)
                if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                and isinstance(n.func.value, ast.Name) and n.func.value.id == "os"
                and n.func.attr == "getenv" and n.args and isinstance(n.args[0], ast.Constant)]
        refs += [n.slice.value for n in ast.walk(node.value) if isinstance(n, ast.Subscript)
                 and isinstance(n.value, ast.Attribute) and isinstance(n.value.value, ast.Name)
                 and n.value.value.id == "os" and n.value.attr == "environ"
                 and isinstance(n.slice, ast.Constant)]
        require(len(set(refs)) == 1, "SETTING_DECLARATION_CHANGED")
        # This is repository Python source, not a user-provided expression or contract field.
        expression = node.value
        if (isinstance(expression, ast.Call) and isinstance(expression.func, ast.Name)
                and expression.func.id == "_preparation_secret_field"):
            expected = ast.parse('_preparation_secret_field(default_factory=lambda: os.getenv("API_POOL_PREPARATION_TOKEN", ""), repr=False)', mode="eval").body
            require(node.target.id == "api_pool_preparation_token"
                    and ast.dump(expression, include_attributes=False) == ast.dump(expected, include_attributes=False),
                    "SECRET_SETTING_DECLARATION_CHANGED")
            expression = next(k.value.body for k in expression.keywords if k.arg == "default_factory")
        value = eval(compile(ast.Expression(expression), str(path), "eval"),
                     {"__builtins__": {}, "os": empty, "int": int})
        require(type(value) in (str, int, bool, type(None)), "SETTING_TYPE_UNSUPPORTED")
        result[node.target.id] = {"env": refs[0], "default": None if node.target.id in BINDINGS else value}
    require(set(BINDINGS) | {"environment"} <= set(result), "SETTING_LAYOUT_INCOMPLETE")
    return result


def inventory(root):
    root = Path(root)
    app = {p.relative_to(root).as_posix(): source_hash(p.read_bytes())
           for p in sorted((root / "src/ticketing").rglob("*.py"))}
    inputs = {p.relative_to(root).as_posix(): source_hash(p.read_bytes())
              for p in [root / "pyproject.toml", root / "requirements.lock"]}
    schema = {p.relative_to(root).as_posix(): source_hash(p.read_bytes())
              for p in sorted((root / "migrations").rglob("*.sql"))}
    require(bool(app) and bool(schema), "INVENTORY_EMPTY")
    return {"application": app, "dependencies": inputs, "migrations": schema}


def draft(root):
    layout = settings_layout(Path(root) / "src/ticketing/config.py")
    values = {k: v["default"] for k, v in layout.items() if k not in BINDINGS and k != "environment"}
    command = ["uvicorn", "ticketing.api:app", "--host", "0.0.0.0", "--port", "8000",
               "--limit-concurrency", "256", "--timeout-keep-alive", "5"]
    return {"format": "ticketing-release/v2", "id": "workspace-reference", "status": "DRAFT",
            "basis": "LOCAL_DEFAULTS_NOT_CLOUD_CANDIDATE", "image": None,
            "inventory": inventory(root), "settings_layout": layout,
            "roles": {"api": {"settings": values, "command": command, "limits": None,
                             "runtime": {"http_success_logs": "1", "api_url": False, "run_identity": "none"}}},
            "protocols": {}, "guardrails": dict(GUARDRAILS)}


def environment_template():
    return {"format": "ticketing-environment/v2", "id": "development-reference",
            "stage": "development", "bindings": {key: {"env": "TICKETING_BIND_" + name}
                                                     for key, name in BINDINGS.items()}, "runtime_bindings": {}}


def validate(release, environment, *, bound=True):
    keys(release, "format id status basis image inventory settings_layout roles protocols guardrails", "RELEASE_KEYS")
    keys(environment, "format id stage bindings runtime_bindings", "ENVIRONMENT_KEYS")
    require(release["format"] == "ticketing-release/v2" and environment["format"] == "ticketing-environment/v2",
            "VERSION_UNSUPPORTED")
    require(all(type(v) is str and NAME.fullmatch(v) for v in (release["id"], environment["id"])), "ID_INVALID")
    require(environment["stage"] in STAGES, "STAGE_INVALID")
    require(release["guardrails"] == GUARDRAILS, "GUARDRAIL_DRIFT")
    require(release["status"] in ("DRAFT", "BOUND"), "RELEASE_STATUS")
    if bound:
        require(release["status"] == "BOUND", "IMAGE_BINDING_REQUIRED")
    if release["status"] == "BOUND":
        require(release["basis"] == "SOURCE_IMAGE_AND_LIMITS_VERIFIED", "CANDIDATE_BASIS_REQUIRED")
        keys(release["image"], "reference root platform config", "IMAGE_KEYS")
        image = release["image"]
        require(all(type(image[k]) is str and DIGEST.fullmatch(image[k]) for k in ("root", "platform", "config")),
                "IMAGE_DIGEST_INVALID")
        require(type(image["reference"]) is str and (image["reference"] == image["root"]
                or (image["reference"].endswith("@" + image["root"])
                    and re.fullmatch(r"[a-z0-9][a-z0-9./:_-]*@sha256:[0-9a-f]{64}", image["reference"]))),
                "IMMUTABLE_IMAGE_REQUIRED")
    else:
        require(release["image"] is None and release["basis"] == "LOCAL_DEFAULTS_NOT_CLOUD_CANDIDATE",
                "DRAFT_MUST_NOT_CLAIM_IMAGE")
    require(release["settings_layout"] == settings_layout(), "SETTING_LAYOUT_DRIFT")
    keys(release["inventory"], "application dependencies migrations", "INVENTORY_KEYS")
    for category, entries in release["inventory"].items():
        require(type(entries) is dict and bool(entries), "INVENTORY_EMPTY")
        for name, digest in entries.items():
            require(type(name) is str and "\\" not in name and not name.startswith("/")
                    and all(p not in ("", ".", "..") for p in name.split("/"))
                    and type(digest) is str and re.fullmatch("[0-9a-f]{64}", digest), "INVENTORY_INVALID")
            require((category == "application" and name.startswith("src/ticketing/") and name.endswith(".py"))
                    or (category == "migrations" and name.startswith("migrations/") and name.endswith(".sql"))
                    or (category == "dependencies" and name in ("pyproject.toml", "requirements.lock")), "INVENTORY_PATH")
    keys(environment["bindings"], " ".join(BINDINGS), "BINDINGS_REQUIRED")
    refs = []
    for ref in environment["bindings"].values():
        keys(ref, "env", "REFERENCE_ONLY_NO_SECRET_LITERAL")
        require(type(ref["env"]) is str and ENV_NAME.fullmatch(ref["env"]) and ref["env"].startswith("TICKETING_BIND_")
                and not ref["env"].startswith("TICKETING_BIND_POD_UID_"), "ENV_REFERENCE_INVALID")
        refs.append(ref["env"])
    require(len(refs) == len(set(refs)), "BINDING_REFERENCE_COLLISION")
    require(type(release["roles"]) is dict and bool(release["roles"]), "ROLES_REQUIRED")
    layout = release["settings_layout"]
    for role, spec in release["roles"].items():
        require(type(role) is str and NAME.fullmatch(role), "ROLE_INVALID")
        role_keys = "settings command limits runtime"
        if "replicas" in spec:
            role_keys += " replicas"
            require(type(spec["replicas"]) is int and 1 <= spec["replicas"] <= 64, "REPLICA_COUNT_INVALID")
        if "baseline_resources" in spec:
            role_keys += " baseline_resources"
        keys(spec, role_keys, "ROLE_KEYS")
        keys(spec["runtime"], "http_success_logs api_url run_identity", "RUNTIME_POLICY_KEYS")
        require(type(spec["runtime"]["http_success_logs"]) is str
                and spec["runtime"]["http_success_logs"] in ("0", "1"), "LOG_POLICY_INVALID")
        require(type(spec["runtime"]["api_url"]) is bool
                and spec["runtime"]["run_identity"] in ("none", "required"), "RUNTIME_POLICY_INVALID")
        expected = set(layout) - set(BINDINGS) - {"environment"}
        require(type(spec["settings"]) is dict and set(spec["settings"]) == expected, "ALL_SETTINGS_REQUIRED")
        for field, value in spec["settings"].items():
            default = layout[field]["default"]
            require(type(value) is type(default) or (field == "pool_max_waiting" and type(value) is int),
                    "SETTING_TYPE_MISMATCH")
            if field == "api_pool_preparation_token":
                require(value == "" and spec["settings"]["api_pool_dispatch_preparation"] is False,
                        "PREPARATION_TOKEN_RUNTIME_ONLY")
            elif type(value) is str:
                require(bool(re.fullmatch("[a-z0-9_-]{1,64}", value)), "SETTING_VALUE_INVALID")
        require(type(spec["command"]) is list and bool(spec["command"])
                and all(type(v) is str and re.fullmatch("[a-zA-Z0-9_.:/=-]{1,128}", v)
                        for v in spec["command"]), "COMMAND_INVALID")
        if release["status"] == "BOUND":
            keys(spec["limits"], "cpus memory_bytes pids", "LIMIT_BINDING_REQUIRED")
            if all(v is None for v in spec["limits"].values()):
                baseline = spec.get("baseline_resources")
                keys(baseline, "capture_sha256 original_ids host_config", "OBSERVED_BASELINE_REQUIRED")
                require(type(baseline["capture_sha256"]) is str
                        and re.fullmatch("[0-9a-f]{64}", baseline["capture_sha256"]), "BASELINE_CAPTURE_INVALID")
                ids = baseline["original_ids"]
                require(type(ids) is list and len(ids) == spec.get("replicas", 1) and len(set(ids)) == len(ids)
                        and all(type(v) is str and re.fullmatch("[0-9a-f]{64}", v) for v in ids), "BASELINE_ORIGINALS_INVALID")
                raw_limits = baseline["host_config"]
                keys(raw_limits, "NanoCpus CpuQuota CpuPeriod Memory MemorySwap PidsLimit", "BASELINE_RESOURCE_KEYS")
                require(all(type(raw_limits[k]) is int and raw_limits[k] == 0
                            for k in ("NanoCpus", "CpuQuota", "CpuPeriod", "Memory", "MemorySwap"))
                        and raw_limits["PidsLimit"] is None, "BASELINE_RESOURCE_DRIFT")
            else:
                require("baseline_resources" not in spec, "UNEXPECTED_BASELINE_RESOURCES")
                require(type(spec["limits"]["cpus"]) is str and re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", spec["limits"]["cpus"])
                        and 0 < float(spec["limits"]["cpus"]) <= 64
                        and all(type(spec["limits"][k]) is int and spec["limits"][k] > 0
                                for k in ("memory_bytes", "pids")), "LIMIT_INVALID")
        else:
            require(spec["limits"] is None, "DRAFT_LIMITS_UNMEASURED")
        # Reuse the application's existing semantic validation, without startup I/O.
        from ticketing.config import Settings
        Settings(**spec["settings"], **{k: "contract-reference-not-secret" for k in BINDINGS},
                 environment=environment["stage"]).validate_values()
    original_ids = [ident for spec in release["roles"].values()
                    for ident in spec.get("baseline_resources", {}).get("original_ids", [])]
    require(len(original_ids) == len(set(original_ids)), "CROSS_ROLE_ORIGINAL_DUPLICATE")
    expected_runtime = {"api_url"} if any(v["runtime"]["api_url"] for v in release["roles"].values()) else set()
    require(type(environment["runtime_bindings"]) is dict
            and set(environment["runtime_bindings"]) == expected_runtime, "RUNTIME_BINDINGS_REQUIRED")
    for ref in environment["runtime_bindings"].values():
        keys(ref, "env", "REFERENCE_ONLY_NO_ENDPOINT_LITERAL")
        require(type(ref["env"]) is str and ENV_NAME.fullmatch(ref["env"])
                and ref["env"].startswith("TICKETING_BIND_")
                and not ref["env"].startswith("TICKETING_BIND_POD_UID_"), "ENV_REFERENCE_INVALID")
        refs.append(ref["env"])
    require(len(refs) == len(set(refs)), "BINDING_REFERENCE_COLLISION")
    require(type(release["protocols"]) is dict, "PROTOCOLS_INVALID")
    for name, protocol in release["protocols"].items():
        require(NAME.fullmatch(name) is not None, "PROTOCOL_NAME")
        keys(protocol, "version operations sources", "PROTOCOL_KEYS")
        require(type(protocol["version"]) is int and protocol["version"] > 0
                and type(protocol["operations"]) is list and bool(protocol["operations"])
                and all(v in ("PREPARE", "COMPARE") for v in protocol["operations"])
                and type(protocol["sources"]) is dict and bool(protocol["sources"]), "PROTOCOL_INVALID")
        for path, digest in protocol["sources"].items():
            require(type(path) is str and path.startswith("scripts/") and path.endswith(".py")
                    and "\\" not in path and all(v not in ("", ".", "..") for v in path.split("/"))
                    and type(digest) is str and re.fullmatch("[0-9a-f]{64}", digest), "PROTOCOL_SOURCE_INVALID")


def bundle(release, environment, *, bound=True):
    validate(release, environment, bound=bound)
    body = {"format": "ticketing-bundle/v2", "release": release, "environment": environment}
    return {**body, "sha256": seal(body)}


def check_bundle(value, *, bound=True):
    keys(value, "format release environment sha256", "BUNDLE_KEYS")
    require(value["format"] == "ticketing-bundle/v2"
            and value["sha256"] == seal({k: v for k, v in value.items() if k != "sha256"}), "BUNDLE_SEAL_DRIFT")
    validate(value["release"], value["environment"], bound=bound)


def verify_workspace(value, root):
    require(value["release"]["inventory"] == inventory(root), "WORKSPACE_SOURCE_SCHEMA_DRIFT")


def expected_service(value, role, *, run=None):
    check_bundle(value)
    require(role in value["release"]["roles"], "UNKNOWN_ROLE")
    release, env = value["release"], value["environment"]
    spec = release["roles"][role]
    if run is not None:
        check_run(value, run)
    variables = {release["settings_layout"][k]["env"]: encode(v) for k, v in spec["settings"].items() if v is not None}
    variables["ENVIRONMENT"] = env["stage"]
    variables.update({BINDINGS[k]: "${" + ref["env"] + ":?required by contract}"
                      for k, ref in env["bindings"].items()})
    variables.update({ref["env"]: "${" + ref["env"] + ":?required by contract}"
                      for ref in env["bindings"].values()})
    variables.update({"TICKETING_CONTRACT_PATH": "/run/ticketing-contract.json",
                      "TICKETING_CONTRACT_SHA256": value["sha256"], "TICKETING_CONTRACT_ROLE": role})
    policy = spec["runtime"]
    variables.update({"HTTP_SUCCESS_LOGS": policy["http_success_logs"], "API_URL": "",
                      "CCE_RUN_ID": "", "CCE_POD_UID": ""})
    if policy["api_url"]:
        ref = env["runtime_bindings"]["api_url"]["env"]
        variables.update({"API_URL": "${" + ref + ":?required by runtime contract}",
                          ref: "${" + ref + ":?required by runtime contract}"})
    if policy["run_identity"] == "required":
        require(run is not None, "RUN_BINDING_REQUIRED")
        uid_ref = instance_reference(role)
        variables.update({"TICKETING_RUN_PATH": "/run/ticketing-run.json", "TICKETING_RUN_SHA256": run["sha256"],
                          "CCE_RUN_ID": run["run_id"], "CCE_POD_UID": "${" + uid_ref + ":?required by run contract}",
                          uid_ref: "${" + uid_ref + ":?required by run contract}"})
    result = {"image": release["image"]["reference"], "command": spec["command"], "environment": variables}
    if spec["limits"]["cpus"] is not None:
        result.update(cpus=spec["limits"]["cpus"], mem_limit=spec["limits"]["memory_bytes"],
                      pids_limit=spec["limits"]["pids"])
    if "replicas" in spec:
        result["deploy"] = {"replicas": spec["replicas"]}
    return result


def encode(value):
    if type(value) is bool:
        return "1" if value else "0"
    return str(value)


def compose(value, *, run=None):
    check_bundle(value)
    services = {}
    if run is not None:
        check_run(value, run)
    for role, spec in value["release"]["roles"].items():
        services[role] = {**expected_service(value, role, run=run), "restart": "no",
                          "volumes": [{"type": "bind", "source": "./bundle.json",
                                       "target": "/run/ticketing-contract.json", "read_only": True}]}
        if spec["runtime"]["run_identity"] == "required":
            services[role]["volumes"].append({"type": "bind", "source": "./run.json",
                                             "target": "/run/ticketing-run.json", "read_only": True})
    return {"name": "ticketing-contract-" + value["environment"]["id"], "services": services}


def preflight(value, observed, experiment, root, *, run=None):
    """Readback comparison only. Does not claim, SSH, authenticate or grant authority."""
    check_bundle(value)
    verify_workspace(value, root)
    names = "protocol version operations source_sha256 bundle_sha256"
    if run is not None:
        check_run(value, run)
        names += " run_sha256"
    keys(experiment, names, "EXPERIMENT_KEYS")
    if run is not None:
        require(experiment["run_sha256"] == run["sha256"], "EXPERIMENT_RUN_DRIFT")
    require(experiment["bundle_sha256"] == value["sha256"], "EXPERIMENT_BUNDLE_DRIFT")
    protocol = value["release"]["protocols"].get(experiment["protocol"])
    require(protocol is not None and protocol["version"] == experiment["version"]
            and protocol["operations"] == experiment["operations"]
            and seal(protocol["sources"]) == experiment["source_sha256"], "RUNNER_PROTOCOL_MISMATCH")
    for name, digest in protocol["sources"].items():
        require(source_hash((Path(root) / name).read_bytes()) == digest, "RUNNER_SOURCE_DRIFT")
    require(observed == {role: expected_service(value, role, run=run) for role in value["release"]["roles"]},
            "DEPLOYMENT_DRIFT")
    return {"pass": True, "scope": "DEPLOYMENT_PLAN_EQUALITY_ONLY", "bundle_sha256": value["sha256"],
            "execution_authorized": False, "live_readiness_passed": False}


def validate_startup(settings, environ=None):
    env = os.environ if environ is None else environ
    path, digest, role = (env.get(k) for k in ("TICKETING_CONTRACT_PATH", "TICKETING_CONTRACT_SHA256", "TICKETING_CONTRACT_ROLE"))
    if not any((path, digest, role)):
        validate_runtime(env)
        return  # Explicit compatibility for historical images/legacy Compose; not contract-qualified.
    require(all((path, digest, role)), "STARTUP_BINDING_INCOMPLETE")
    value = load(path)
    check_bundle(value)
    require(digest == value["sha256"], "STARTUP_SEAL_DRIFT")
    require(role in value["release"]["roles"], "UNKNOWN_ROLE")
    validate_runtime(env)
    require(settings.environment == value["environment"]["stage"], "STARTUP_STAGE_DRIFT")
    actual = asdict(settings)
    for name, expected in value["release"]["roles"][role]["settings"].items():
        require(type(actual[name]) is type(expected) and actual[name] == expected, "STARTUP_SETTING_DRIFT_" + name.upper())
    for name, ref in value["environment"]["bindings"].items():
        require(bool(env.get(ref["env"])) and actual[name] == env[ref["env"]], "STARTUP_REFERENCE_DRIFT_" + name.upper())
    package = Path(__file__).parent
    installed = {"src/ticketing/" + p.relative_to(package).as_posix(): source_hash(p.read_bytes())
                 for p in package.rglob("*.py")}
    require(installed == value["release"]["inventory"]["application"], "INSTALLED_APPLICATION_DRIFT")


def instance_reference(role):
    return "TICKETING_BIND_POD_UID_" + role.upper().replace("-", "_")


def run_binding(value, run_id, instances):
    check_bundle(value)
    body = {"format": "ticketing-run/v1", "bundle_sha256": value["sha256"],
            "run_id": run_id, "instances": instances}
    result = {**body, "sha256": seal(body)}
    check_run(value, result)
    return result


def check_run(value, run):
    keys(run, "format bundle_sha256 run_id instances sha256", "RUN_KEYS")
    require(run["format"] == "ticketing-run/v1"
            and run["sha256"] == seal({k: v for k, v in run.items() if k != "sha256"}), "RUN_SEAL_DRIFT")
    require(run["bundle_sha256"] == value["sha256"], "RUN_BUNDLE_DRIFT")
    require(type(run["run_id"]) is str and re.fullmatch("[a-z][a-z0-9-]{1,95}", run["run_id"]), "RUN_ID_INVALID")
    required = {role for role, spec in value["release"]["roles"].items()
                if spec["runtime"]["run_identity"] == "required"}
    require(type(run["instances"]) is dict and bool(required) and bool(run["instances"]), "RUN_INSTANCES_REQUIRED")
    for uid, role in run["instances"].items():
        require(type(uid) is str and re.fullmatch("[a-zA-Z0-9][a-zA-Z0-9-]{0,127}", uid)
                and type(role) is str and role in required, "RUN_INSTANCE_ROLE_INVALID")
    require(set(run["instances"].values()) == required, "RUN_ROLE_COVERAGE_REQUIRED")


def _endpoint(value):
    require(type(value) is str and bool(value) and not any(c.isspace() for c in value), "API_URL_INVALID")
    try:
        parsed = urlsplit(value)
        valid = (parsed.scheme in ("http", "https") and bool(parsed.hostname)
                 and parsed.username is None and parsed.password is None
                 and not parsed.query and not parsed.fragment
                 and (parsed.port is None or 1 <= parsed.port <= 65535))
    except ValueError:
        valid = False
    require(valid, "API_URL_INVALID")
    return value


# Each key pins an immutable metadata/endpoint/instance snapshot. No credentials cached.
# Callback reads check environment values against it without filesystem I/O.
_RUNTIME_CACHE = {}


def _runtime_state(env):
    path, digest, role = (env.get(k) for k in
                         ("TICKETING_CONTRACT_PATH", "TICKETING_CONTRACT_SHA256", "TICKETING_CONTRACT_ROLE"))
    if not any((path, digest, role)):
        require(not any(env.get(k) for k in ("TICKETING_RUN_PATH", "TICKETING_RUN_SHA256")),
                "RUN_WITHOUT_CONTRACT")
        return None
    require(all((path, digest, role)), "STARTUP_BINDING_INCOMPLETE")
    run_path, run_sha = env.get("TICKETING_RUN_PATH"), env.get("TICKETING_RUN_SHA256")
    key = (path, digest, role, run_path, run_sha)
    if key in _RUNTIME_CACHE:
        return _RUNTIME_CACHE[key]
    value = load(path)
    check_bundle(value)
    require(digest == value["sha256"], "STARTUP_SEAL_DRIFT")
    require(role in value["release"]["roles"], "UNKNOWN_ROLE")
    policy = value["release"]["roles"][role]["runtime"]
    state = {"policy": policy, "role": role, "api_url": None, "endpoint_ref": None,
             "run_id": "", "pod_uid": ""}
    if policy["api_url"]:
        name = value["environment"]["runtime_bindings"]["api_url"]["env"]
        state.update(endpoint_ref=name, api_url=_endpoint(env.get(name)))
    if policy["run_identity"] == "required":
        require(bool(run_path) and bool(run_sha), "RUN_BINDING_REQUIRED")
        run = load(run_path)
        check_run(value, run)
        require(run_sha == run["sha256"], "STARTUP_RUN_SEAL_DRIFT")
        pod = env.get("CCE_POD_UID")
        require(type(pod) is str and run["instances"].get(pod) == role, "STARTUP_RUN_INSTANCE_DRIFT")
        state.update(run_id=run["run_id"], pod_uid=pod)
    else:
        require(not run_path and not run_sha and not env.get(instance_reference(role)), "UNEXPECTED_RUN_BINDING")
    _check_runtime(state, env)
    require(len(_RUNTIME_CACHE) < 32, "RUNTIME_BINDING_CACHE_FULL")
    return _RUNTIME_CACHE.setdefault(key, state)


def _check_runtime(state, env):
    policy = state["policy"]
    require(env.get("HTTP_SUCCESS_LOGS", "1") == policy["http_success_logs"], "LOG_POLICY_DRIFT")
    if policy["api_url"]:
        require(env.get("API_URL") == env.get(state["endpoint_ref"]) == state["api_url"], "API_URL_DRIFT")
    else:
        require(not env.get("API_URL"), "UNDECLARED_API_URL")
    require(env.get("CCE_RUN_ID", "") == state["run_id"]
            and env.get("CCE_POD_UID", "") == state["pod_uid"], "STARTUP_RUN_IDENTITY_DRIFT")
    if policy["run_identity"] == "required":
        require(env.get(instance_reference(state["role"])) == state["pod_uid"], "STARTUP_INSTANCE_REFERENCE_DRIFT")


def validate_runtime(environ=None):
    env = os.environ if environ is None else environ
    state = _runtime_state(env)
    if state is not None:
        _check_runtime(state, env)
    return state


def runtime_value(name, default, environ=None):
    require(name in ("API_URL", "HTTP_SUCCESS_LOGS"), "RUNTIME_VALUE_UNSUPPORTED")
    env = os.environ if environ is None else environ
    state = validate_runtime(env)
    if state is None:
        return env.get(name, default)
    if name == "API_URL":
        require(state["policy"]["api_url"], "UNDECLARED_API_URL")
        return state["api_url"]
    return state["policy"]["http_success_logs"]
