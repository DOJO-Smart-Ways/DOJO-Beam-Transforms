import os
import importlib
import inspect
import apache_beam as beam
 
class AutoRunPipeline(beam.DoFn):
    def __init__(self, pipeline, path, pattern=None, excludePattern=None):
        self.path = path
        self.patterns = pattern if isinstance(pattern, list) else [pattern] if pattern else []
        self.exclude_patterns = excludePattern if isinstance(excludePattern, list) else [excludePattern] if excludePattern else []
        self.pipeline = pipeline
 
    def process(self):
        for root, dirs, files in os.walk(self.path):
            for file in files:
                if file.endswith(".py") and not file.startswith("__"):
                    relative_path = os.path.relpath(root, self.path)
                    module_name = os.path.join(relative_path, file[:-3]).replace(os.sep, ".")
                    base_path = self.path.replace(os.sep, ".")
                    module_name = f'{base_path}.{module_name}'.replace("...", ".").replace("..", ".")
                   
                    try:
                        module = importlib.import_module(module_name)
                    except ImportError as e:
                        print(f"Erro ao importar {module_name}: {e}")
                        continue
 
                    class_name = file[:-3]
                   
                    if self.patternCheck(class_name):
                        if hasattr(module, class_name):
                            classe = getattr(module, class_name)
                            if inspect.isclass(classe):
                                try:
                                    instancia = classe()
                                    instancia.run(self.pipeline)
                                except AttributeError as e:
                                    print(f"Erro: {e}")
                                    print(f"A classe {class_name} não possui o método 'run'.")
                        else:
                            print(f"A classe {class_name} não foi encontrada no módulo {module_name}.")
 
    def patternCheck(self, class_name):
        if any(pat in class_name for pat in self.exclude_patterns):
            return False
        return not self.patterns or any(pat in class_name for pat in self.patterns)
 
 