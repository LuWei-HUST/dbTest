import os

def getHomeDir():
    return os.path.dirname(os.path.abspath(__file__))

if __name__ == "__main__":
    print(getHomeDir())