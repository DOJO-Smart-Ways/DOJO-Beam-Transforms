import apache_beam as beam

import os
import importlib
import inspect

class AutoRunPipeline(beam.DoFn):
    def __init__(self, pipeline, path, pattern=None, excludePattern=None):
        self.path = path
        self.pattern = pattern
        self.excludePattern = excludePattern
        self.pipeline = pipeline

    def process(self):
        for root, dirs, files in os.walk(self.path):
            # print(f"\nDiretório atual: {root}")
            # print(f"Subdiretórios: {dirs}")
            # print(f"Arquivos: {files}")
            for file in files:
                #print(f"A file {file}")
                if file.endswith(".py") and not file.startswith("__"):
                    caminho_relativo = os.path.relpath(root, self.path)
                    #print(f"A caminho_relativo {caminho_relativo}")
                    nome_modulo = os.path.join(caminho_relativo, file[:-3]).replace(os.sep, ".")
                    #print(f"A nome_modulo {nome_modulo}")
                    base_path = self.path.replace(os.sep, ".")
                    #print(f"A base_path {base_path}")
                    nome_modulo = f'{base_path}.{nome_modulo}'.replace("...", ".").replace("..", ".")
                    #print(f"A nome_modulo {nome_modulo}")
                    try:
                        modulo = importlib.import_module(nome_modulo)
                        #print(f"A modulo {modulo}")
                    except ImportError as e:
                        print(f"Erro ao importar {nome_modulo}: {e}")
                        continue

                    nome_classe = file[:-3]
                    #print(f"A nome_classe {nome_classe}")
                    if self.patternCheck(nome_classe):
                        if hasattr(modulo, nome_classe):
                            classe = getattr(modulo, nome_classe)
                            if inspect.isclass(classe):
                                try:
                                    instancia = classe()
                                    #print(f"A classe {nome_classe} possui o método 'run'.")
                                    instancia.run(self.pipeline)
                                except AttributeError as e:
                                    print(f"Erro: {e}")
                                    print(f"A classe {nome_classe} não possui o método 'run'.")
                        else:
                            print(f"A classe {nome_classe} não foi encontrada no módulo {nome_modulo}.")

    """
    Return True if checks class name and name is according pattern or exclude pattern
    """
    def patternCheck(self, nome_classe):

        if self.excludePattern and self.excludePattern in nome_classe:
            print("patternCheck false")
            return False
        
        return self.pattern is None or self.pattern in nome_classe