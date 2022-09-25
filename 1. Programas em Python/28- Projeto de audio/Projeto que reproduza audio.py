import pygame

pygame.mixer.init()
pygame.init()
pygame.mixer.music.load('musicasite.mp3')
pygame.music.play()
pygame.event.wait()
input()