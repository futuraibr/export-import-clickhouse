"""
Controle de progresso usado pelos fluxos: mover arquivos de dump pra made depois
de processados e manter o .enviados.log (resumo do que já foi enviado, que vai
somando a cada execução).
"""

import os
import shutil

SUMMARY_MARKER = "═" * 57


def move_to_processed(path: str, src_dir: str, dst_dir: str) -> str | None:
    """Move o arquivo de dump pra made mantendo as subpastas (ex.: por mês)."""
    abs_path = os.path.abspath(path)
    abs_src  = os.path.abspath(src_dir)
    if os.path.commonpath([abs_path, abs_src]) != abs_src:
        return None
    rel  = os.path.relpath(abs_path, abs_src)
    dest = os.path.join(dst_dir, rel)
    os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)
    shutil.move(abs_path, dest)
    return dest


def already_processed(path: str, src_dir: str, dst_dir: str) -> bool:
    """Diz se o arquivo já está em made (ou seja, já foi processado antes)."""
    abs_path = os.path.abspath(path)
    abs_src  = os.path.abspath(src_dir)
    if os.path.commonpath([abs_path, abs_src]) != abs_src:
        return False
    return os.path.exists(os.path.join(dst_dir, os.path.relpath(abs_path, abs_src)))


def _to_int(s: str) -> int:
    return int(s.strip().replace(",", "").replace(".", "") or 0)


def load_enviados(log_path: str) -> list[dict]:
    """Lê o log atual pra não perder o histórico das execuções anteriores."""
    if not os.path.exists(log_path):
        return []
    entries, cur = [], None
    with open(log_path) as f:
        for line in f:
            line = line.rstrip()
            if line.startswith(SUMMARY_MARKER):
                break
            if line.startswith("arquivo:"):
                if cur:
                    entries.append(cur)
                cur = {"file": line.split(":", 1)[1].strip(), "read": 0, "inserted": 0, "discarded": 0, "elapsed": ""}
            elif cur is None:
                continue
            elif "Concluído em" in line:
                cur["elapsed"] = line.split(":", 1)[1].strip()
            elif "Linhas lidas" in line:
                cur["read"] = _to_int(line.split(":", 1)[1])
            elif "Linhas inseridas" in line:
                cur["inserted"] = _to_int(line.split(":", 1)[1])
            elif "Linhas descart" in line:
                cur["discarded"] = _to_int(line.split(":", 1)[1])
    if cur:
        entries.append(cur)
    return entries


def upsert_entry(entries: list[dict], entry: dict):
    """Adiciona o arquivo no log, ou atualiza se ele já estiver lá."""
    for i, e in enumerate(entries):
        if e["file"] == entry["file"]:
            entries[i] = entry
            return
    entries.append(entry)


def write_enviados_log(log_path: str, entries: list[dict]):
    """Regrava o .enviados.log com a lista de arquivos e um total no fim."""
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    total_read     = sum(e["read"] for e in entries)
    total_inserted = sum(e["inserted"] for e in entries)
    total_discard  = sum(e["discarded"] for e in entries)

    with open(log_path, "w") as f:
        for e in entries:
            f.write(f"arquivo: {e['file']}\n\n")
            f.write(f"  Concluído em    : {e['elapsed']}\n")
            f.write(f"  Linhas lidas    : {e['read']:,}\n")
            f.write(f"  Linhas inseridas: {e['inserted']:,}\n")
            f.write(f"  Linhas descart. : {e['discarded']:,}\n")
            f.write("\n")
        f.write(f"{SUMMARY_MARKER}\n")
        f.write("RESUMO TOTAL\n\n")
        f.write(f"  Arquivos ({len(entries)}):\n")
        for e in entries:
            f.write(f"    - {e['file']}\n")
        f.write("\n")
        f.write(f"  Linhas lidas    : {total_read:,}\n")
        f.write(f"  Linhas inseridas: {total_inserted:,}\n")
        f.write(f"  Linhas descart. : {total_discard:,}\n")
        f.write(f"{SUMMARY_MARKER}\n")
