# test_script.py
import sys
import os

# Adiciona o diretório raiz ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils import a, b, c

a()  # Saída: Função a
b()  # Saída: Função b
c()  # Saída: Função c
