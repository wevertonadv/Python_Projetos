# sempre quando tiver input tipo primitivo é string
# a = é o objeto  e os que tem (), são os metodos

a = input("Digite Algo: ")
print("O tipo primitivo de",a," é:",type(a))
print("Só tem espaços?",a.isspace())
print("é Numerico",a.isnumeric())
print("é alfabetico",a.isalpha())
print("é alpha numerico",a.isalnum())
