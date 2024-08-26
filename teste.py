import os

arquivo = open("teste.txt")
conteudo = arquivo.read()

print("tipo de conteudo", type(conteudo))
print("A macedonia is coming")

print(os.path.realpath(arquivo.name))
print(os.path.abspath(arquivo.name))

print(repr (conteudo))

arquivo.close()