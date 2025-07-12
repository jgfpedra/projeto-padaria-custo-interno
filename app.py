import psycopg2
from config.db_vr import get_db_vr
import time
from typing import Dict, Any, List
from psycopg2.extensions import cursor as PgCursor
from psycopg2.extensions import connection as PgConnection

ULTIMO_ID_FILE = "config/ultimo_id_transferencia.txt"

def inicializa_contexto() -> Dict[str, Any]:
    return {
        'id_produto': None,
        'id_loja': None,
        'mercadologico1': None,
        'quantidade': 0.00,
    }

def inicializa_mercadologicos():
    # 102 --> panificacao - transferencia
    # 103 --> confeitaria
    # 112 --> salgado
    return [102, 103, 112]

def get_transacoes(cursor: psycopg2.extensions.cursor):
    cursor.execute("SELECT * FROM transacoes WHERE status = 'pendente'")
    return cursor.fetchall()

def verifica_transacoes():
    return 0

def altera_custo_interno():
    return 0

def ler_ultimo_id_processado() -> int:
    try:
        with open(ULTIMO_ID_FILE, "r") as f:
            return int(f.read().strip())
    except Exception:
        return 0

def salvar_ultimo_id_processado(ultimo_id: int) -> None:
    with open(ULTIMO_ID_FILE, "w") as f:
        f.write(str(ultimo_id))

from typing import Tuple

def get_novas_transferencias(cursor: PgCursor, ultimo_id: int) -> List[Tuple[int, int, float, int]]:
    cursor.execute("SELECT id, id_produto, quantidade, mercadologico1 FROM transferenciainterna WHERE id > %s ORDER BY id ASC", (ultimo_id,))
    return cursor.fetchall()

def produto_eh_interno(id_produto: int, cursor: PgCursor) -> bool:
    # Exemplo: verifica se o produto está na lista de produtos internos
    cursor.execute("SELECT 1 FROM produtos_internos WHERE id_produto = %s", (id_produto,))
    return cursor.fetchone() is not None

def insere_produto_custo_interno(cursor: PgCursor, id_produto: int, quantidade: float, mercadologico: int) -> None:
    cursor.execute(
        "INSERT INTO produtos_custo_interno (id_produto_interno, quantidade, mercadologico) VALUES (%s, %s, %s)",
        (id_produto, quantidade, mercadologico)
    )

def calcula_media_quantidade(cursor: PgCursor, id_produto: int) -> float:
    cursor.execute(
        "SELECT AVG(quantidade) FROM produtos_custo_interno WHERE id_produto_interno = %s",
        (id_produto,)
    )
    result = cursor.fetchone()
    return result[0] if result and result[0] is not None else 0

def atualiza_custo_produto(cursor: PgCursor, id_produto: int, novo_custo: float) -> None:
    cursor.execute(
        "UPDATE produto SET custo_com_imposto = %s WHERE id = %s",
        (novo_custo, id_produto)
    )

def main():
    conn: PgConnection = get_db_vr() # type: ignore
    cursor = conn.cursor() # type: ignore
    assert isinstance(conn, PgConnection)
    assert isinstance(cursor, PgCursor)
    mercadologicos = inicializa_mercadologicos()
    while True:
        ultimo_id = ler_ultimo_id_processado()
        transferencias = get_novas_transferencias(cursor, ultimo_id)
        maior_id = ultimo_id
        for t in transferencias:
            id_transf, id_produto, quantidade, mercadologico = t
            if id_transf > maior_id:
                maior_id = id_transf
            if mercadologico in mercadologicos and produto_eh_interno(id_produto, cursor):
                insere_produto_custo_interno(cursor, id_produto, quantidade, mercadologico)
                media = calcula_media_quantidade(cursor, id_produto)
                atualiza_custo_produto(cursor, id_produto, media)
        conn.commit()
        if maior_id > ultimo_id:
            salvar_ultimo_id_processado(maior_id)
        time.sleep(10)  # Aguarda 10 segundos antes de buscar novamente

if __name__ == "__main__":
    main()
