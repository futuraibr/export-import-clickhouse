import json
import logging

import boto3
from boto3.dynamodb.conditions import Attr

log = logging.getLogger()
log.setLevel(logging.INFO)

ddb = boto3.resource("dynamodb")   # região herdada do ambiente da Lambda
MAX_DEPTH = 100

def _get_process(table_process, process_id):
    resp = table_process.get_item(
        Key={"process_id": process_id},
        ProjectionExpression="process_id, #lvl",
        ExpressionAttributeNames={"#lvl": "level"},
    )
    return resp.get("Item")


def _resolve_unit_id(table_process, process_id, cache):
    if process_id in cache:
        return cache[process_id]
    chain, visited, current = [], set(), process_id
    for _ in range(MAX_DEPTH):
        if current in visited:
            raise RuntimeError(f"ciclo detectado em {current} (origem {process_id})")
        visited.add(current)
        chain.append(current)
        proc = _get_process(table_process, current)
        if proc is None:
            raise LookupError(f"process {current} não encontrado (origem {process_id})")
        if proc.get("level") == "root":
            unit = proc["process_id"]
            for pid in chain:
                cache[pid] = unit
            return unit
        current = proc["level"]
    raise RuntimeError(f"profundidade > {MAX_DEPTH} (origem {process_id})")


def backfill_unit_ids(company_id, apply=True, model_config_type="PREDICTION_TRIGGER", process_id=None):
    # process_id != None => processa só os configs desse process_id (bom pra testar 1 item)
    t_cfg = ddb.Table(f"model_config_{company_id}")
    t_proc = ddb.Table(f"process_{company_id}")
    cache, updated, skipped, errors = {}, 0, 0, 0

    kwargs = {
        "FilterExpression": Attr("active").eq(True) & Attr("type").eq(model_config_type),
        "ProjectionExpression": "process_id, created_at, unit_id",
    }
    while True:
        resp = t_cfg.scan(**kwargs)
        for cfg in resp.get("Items", []):
            if process_id and cfg["process_id"] != process_id:
                continue
            try:
                unit = _resolve_unit_id(t_proc, cfg["process_id"], cache)
            except (LookupError, RuntimeError) as e:
                log.warning("ERRO %s", e)
                errors += 1
                continue
            if cfg.get("unit_id") == unit:
                skipped += 1
                continue
            if apply:
                t_cfg.update_item(
                    Key={"process_id": cfg["process_id"], "created_at": cfg["created_at"]},
                    UpdateExpression="SET unit_id = :u",
                    ExpressionAttributeValues={":u": unit},
                )
            updated += 1
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek

    return {"updated": updated, "skipped": skipped, "errors": errors, "applied": apply}


def lambda_handler(event, context):
    # company_id do token (Cognito) com fallback pro body / event direto
    claims = (event.get("requestContext", {}).get("authorizer", {}).get("claims", {}) or {})
    company_id = claims.get("custom:company_id")

    body = {}
    if event.get("body"):
        body = json.loads(event["body"])
    company_id = company_id or body.get("company_id") or event.get("company_id")

    # apply default True; mande {"apply": false} pra rodar dry-run
    apply = body.get("apply", event.get("apply", True))
    # process_id opcional: processa só esse item (bom pra testar 1)
    process_id = body.get("process_id", event.get("process_id"))

    result = backfill_unit_ids(company_id, apply=apply, process_id=process_id)
    log.info("backfill_unit_ids %s (process_id=%s) -> %s", company_id, process_id, result)
    return {"statusCode": 200, "body": json.dumps(result)}


# alias: funciona tanto com Handler "lambda_function.lambda_handler" (padrão AWS)
# quanto com "lambda_function.handler".
handler = lambda_handler
