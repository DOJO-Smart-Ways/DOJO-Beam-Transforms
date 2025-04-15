import os
import importlib
import inspect

current_dir = os.path.dirname(__file__)
package_name = __name__

for file in os.listdir(current_dir):
    if file.endswith(".py") and file != "__init__.py":
        module_name = file[:-3]
        module = importlib.import_module(f"{package_name}.{module_name}")
        
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ == module.__name__:
                globals()[name] = obj

if __name__ == "__main__":
    raise RuntimeError("The __init__.py file is not meant to be executed directly.")