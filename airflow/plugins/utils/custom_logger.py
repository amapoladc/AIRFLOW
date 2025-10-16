
class CustomLogger:
    @staticmethod
    def reset():
        print("[CustomLogger] reset called")

    @staticmethod
    def emit(id_tarea, nom_tarea, nom_proceso, object_py, object_bd, fail, description):
        # Minimal safe stub. Replace with your production implementation.
        print(f"[CustomLogger] {id_tarea=} {nom_tarea=} {nom_proceso=} {object_py=} {object_bd=} {fail=} {description=}")

    @staticmethod
    def get_records_as_html():
        return "<table><tr><th>Stub</th></tr><tr><td>Replace CustomLogger with your implementation.</td></tr></table>"
