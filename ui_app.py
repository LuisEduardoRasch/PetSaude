import subprocess
import sys
from pathlib import Path
import threading
import tkinter as tk
from tkinter import filedialog, messagebox


def run_processing(input_dir: str, output_dir: str, button: tk.Button) -> None:
    """
    Executa o script principal de processamento em uma thread separada.

    Parameters
    ----------
    input_dir : str
        Caminho da pasta de entrada contendo arquivos .ods/.csv.
    output_dir : str
        Caminho da pasta de saída para os arquivos gerados.
    button : tkinter.Button
        Botão de disparo do processamento, para habilitar/desabilitar durante a execução.
    """
    try:
        button.config(state=tk.DISABLED)

        # Normaliza e valida pastas informadas pelo usuário
        input_dir = input_dir.strip()
        output_dir = output_dir.strip()

        if not input_dir or not output_dir:
            messagebox.showerror(
                "Erro",
                "Informe as pastas de ENTRADA e SAÍDA antes de iniciar o processamento.",
            )
            return

        input_path = Path(input_dir)
        output_path = Path(output_dir)

        if not input_path.is_dir():
            messagebox.showerror(
                "Erro",
                f"Pasta de entrada não encontrada:\n{input_path}",
            )
            return

        if not output_path.exists():
            # Cria pasta de saída se não existir
            try:
                output_path.mkdir(parents=True, exist_ok=True)
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror(
                    "Erro",
                    f"Não foi possível criar a pasta de saída:\n{output_path}\n{exc}",
                )
                return

        # Quando empacotado pelo PyInstaller, chamamos o módulo diretamente.
        if getattr(sys, "frozen", False):
            try:
                import convert_merge_split as cms  # type: ignore[import]
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror(
                    "Erro",
                    f"Não foi possível carregar o módulo de processamento:\n{exc}",
                )
                return

            # Simula chamada via linha de comando para reutilizar o argparse do script.
            old_argv = sys.argv
            sys.argv = [
                "convert_merge_split",
                "--input-dir",
                str(input_path),
                "--output-dir",
                str(output_path),
            ]
            try:
                try:
                    cms.main()  # type: ignore[attr-defined]
                    return_code = 0
                except SystemExit as exc:  # argparse pode chamar sys.exit()
                    return_code = int(exc.code) if isinstance(exc.code, int) else 1
            finally:
                sys.argv = old_argv
        else:
            # Modo desenvolvimento: chama o script via python no terminal.
            script_dir = Path(__file__).resolve().parent
            script_path = script_dir / "convert_merge_split.py"

            if not script_path.exists():
                messagebox.showerror(
                    "Erro",
                    f"Arquivo convert_merge_split.py não encontrado em:\n{script_path}",
                )
                return

            cmd = [
                sys.executable,
                str(script_path),
                "--input-dir",
                str(input_path),
                "--output-dir",
                str(output_path),
            ]

            # NÃO captura stdout/stderr -> logs aparecem no terminal
            result = subprocess.run(cmd)
            return_code = result.returncode

        if return_code == 0:
            messagebox.showinfo("Concluído", "Processamento concluído com sucesso.")
        else:
            messagebox.showerror(
                "Erro",
                f"Erro no processamento (código {return_code}). "
                "Veja os detalhes no terminal (modo desenvolvimento).",
            )

    except Exception as exc:  # noqa: BLE001
        messagebox.showerror("Erro", str(exc))
    finally:
        button.config(state=tk.NORMAL)


def choose_input_dir(entry: tk.Entry) -> None:
   """Abre diálogo para escolha da pasta de entrada e atualiza o campo de texto."""
   path = filedialog.askdirectory(title="Selecione a pasta de entrada (.ods/.csv)")
   if path:
       entry.delete(0, tk.END)
       entry.insert(0, path)


def choose_output_dir(entry: tk.Entry) -> None:
   """Abre diálogo para escolha da pasta de saída e atualiza o campo de texto."""
   path = filedialog.askdirectory(title="Selecione a pasta de saída")
   if path:
       entry.delete(0, tk.END)
       entry.insert(0, path)


def main() -> None:
   """Inicializa a interface gráfica mínima para disparar o processamento."""
   root = tk.Tk()
   root.title("PetSaude - Processamento de Altas Hospitalares")
   root.geometry("600x160")

   frame_params = tk.Frame(root, padx=10, pady=10)
   frame_params.pack(fill=tk.BOTH, expand=True)

   # Pasta de entrada
   tk.Label(frame_params, text="Pasta de entrada (.ods/.csv):").grid(
       row=0, column=0, sticky="w"
   )
   entry_input = tk.Entry(frame_params, width=50)
   entry_input.grid(row=0, column=1, padx=5, pady=2)
   btn_input = tk.Button(
       frame_params,
       text="Selecionar...",
       command=lambda: choose_input_dir(entry_input),
   )
   btn_input.grid(row=0, column=2, padx=5, pady=2)

   # Pasta de saída
   tk.Label(frame_params, text="Pasta de saída:").grid(
       row=1, column=0, sticky="w", pady=(5, 0)
   )
   entry_output = tk.Entry(frame_params, width=50)
   entry_output.grid(row=1, column=1, padx=5, pady=(5, 2))
   btn_output = tk.Button(
       frame_params,
       text="Selecionar...",
       command=lambda: choose_output_dir(entry_output),
   )
   btn_output.grid(row=1, column=2, padx=5, pady=(5, 2))

   # Botão de processamento
   frame_actions = tk.Frame(root, padx=10, pady=5)
   frame_actions.pack(fill=tk.X)

   btn_process = tk.Button(
       frame_actions,
       text="Iniciar processamento",
       width=20,
       command=lambda: threading.Thread(
           target=run_processing,
           args=(entry_input.get(), entry_output.get(), btn_process),
           daemon=True,
       ).start(),
   )
   btn_process.pack(side=tk.LEFT)

   root.mainloop()


if __name__ == "__main__":
   main()