# Estruturas de repetição, repete quando vezes eu quiser.

    # WHILE: O laço while é conhecido por repetir o mesmo bloco de código desde que uma determinada 
    # condição seja verdadeira.
    
    # WHILE quer dizer Enquanto determinada condição for verdeira <condição>

contador =0
while contador  <= 3:
    a1 = int(input('Digite a 1° nota: '))
    a2 = int(input('Digite a 2° nota: '))
    a3 = int(input('Digite a 3° nota:: '))
    
    print("A média é ",(a1+a2+a3)/3)
    contador +=1


 