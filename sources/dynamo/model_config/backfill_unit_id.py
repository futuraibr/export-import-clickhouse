import argparse
import os
import sys

# acha a raiz do projeto pra conseguir importar o common/
_root = os.path.dirname(os.path.abspath(__file__))
while _root != os.path.dirname(_root) and not os.path.isdir(os.path.join(_root, "common")):
    _root = os.path.dirname(_root)
sys.path.insert(0, _root)

from boto3.dynamodb.conditions import Attr

from common import config
from common.dynamo import get_dynamodb_resource

MAX_DEPTH = 100  # teto de profundidade da hierarquia (proteção extra contra loop)

def get_process(table_process, process_id: str) -> dict | None:
    """Busca o processo e devolve só process_id + level (ou None se não existir)."""
    resp = table_process.get_item(
        Key={"process_id": process_id},
        ProjectionExpression="process_id, #lvl",
        ExpressionAttributeNames={"#lvl": "level"},
    )
    return resp.get("Item")


def resolve_unit_id(table_process, process_id: str, cache: dict) -> str:
    """Sobe a hierarquia via `level` até level == 'root'. Memoiza a cadeia inteira no cache."""
    if process_id in cache:
        return cache[process_id]

    chain: list[str] = []
    visited: set[str] = set()
    current = process_id

    for _ in range(MAX_DEPTH):
        if current in visited:
            raise RuntimeError(f"ciclo detectado em {current} (origem {process_id})")
        visited.add(current)
        chain.append(current)

        proc = get_process(table_process, current)
        if proc is None:
            raise LookupError(f"process {current} não encontrado (origem {process_id})")

        if proc.get("level") == "root":
            unit_id = proc["process_id"]
            for pid in chain:          # toda a cadeia aponta pra mesma unidade
                cache[pid] = unit_id
            return unit_id

        current = proc["level"]

    raise RuntimeError(f"profundidade > {MAX_DEPTH} (origem {process_id})")


def scan_active_configs(table_model_config) -> list[dict]:
    """Scan paginado dos model_config ativos do tipo configurado."""
    items: list[dict] = []
    kwargs = {
        "FilterExpression": Attr("active").eq(True) & Attr("type").eq(config.MODEL_CONFIG_TYPE),
        "ProjectionExpression": "process_id, created_at, unit_id",
    }
    while True:
        resp = table_model_config.scan(**kwargs)
        items.extend(resp.get("Items", []))
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek
    return items


def main():
    parser = argparse.ArgumentParser(
        description="Resolve o unit_id (raiz) de cada model_config ativo e grava no DynamoDB."
    )
    parser.add_argument("--apply", action="store_true",
                        help="grava o unit_id no DynamoDB (sem esta flag = DRY-RUN, não escreve).")
    parser.add_argument("--company-id", default=config.COMPANY_ID,
                        help="sobrescreve o COMPANY_ID do .env.")
    parser.add_argument("--process-id",
                        help="processa só os model_config deste process_id (bom pra testar 1 item).")
    args = parser.parse_args()

    company_id = args.company_id
    cfg_table_name  = f"model_config_{company_id}"
    proc_table_name = f"process_{company_id}"

    print(f"\n{'='*57}")
    print(f"  AWS profile  : {config.AWS_PROFILE or '(credencial ambiente)'}")
    print(f"  Região AWS   : {config.AWS_REGION}")
    print(f"  model_config : {cfg_table_name}")
    print(f"  process      : {proc_table_name}")
    print(f"  Filtro       : active=True, type={config.MODEL_CONFIG_TYPE}")
    print(f"  Modo         : {'APPLY (escreve no DynamoDB)' if args.apply else 'DRY-RUN (não escreve)'}")
    print(f"{'='*57}\n")

    ddb = get_dynamodb_resource()
    t_cfg = ddb.Table(cfg_table_name)
    t_proc = ddb.Table(proc_table_name)

    configs = scan_active_configs(t_cfg)
    if args.process_id:
        configs = [c for c in configs if c["process_id"] == args.process_id]
        print(f"filtrando por process_id={args.process_id}")
    print(f"model_config ativos encontrados: {len(configs)}\n")

    cache: dict[str, str] = {}
    updated = skipped = errors = 0

    for cfg in configs:
        pid = cfg["process_id"]
        created_at = cfg["created_at"]
        try:
            unit_id = resolve_unit_id(t_proc, pid, cache)
        except (LookupError, RuntimeError) as e:
            print(f"  ERRO   {e}")
            errors += 1
            continue

        if cfg.get("unit_id") == unit_id:
            skipped += 1
            continue

        if args.apply:
            t_cfg.update_item(
                Key={"process_id": pid, "created_at": created_at},
                UpdateExpression="SET unit_id = :u",
                ExpressionAttributeValues={":u": unit_id},
            )
        tag = "APPLIED" if args.apply else "DRY-RUN"
        print(f"  {tag}  process {pid} (created_at={created_at}) → unit_id={unit_id}")
        updated += 1

    print(f"\n{'='*57}")
    print(f"  Total ativos      : {len(configs)}")
    print(f"  {'Atualizados' if args.apply else 'A atualizar':<17} : {updated}")
    print(f"  Já corretos       : {skipped}")
    print(f"  Erros             : {errors}")
    print(f"  Modo              : {'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"{'='*57}\n")


if __name__ == "__main__":
    main()
