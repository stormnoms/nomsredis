
    At a top level directory do this:

    alias bgt='mkdir src; cd src; mkdir github.com; cd github.com; mkdir stormasm; cd stormasm'
    bgt
    cd src/github.com/stormasm
    git clone git@github.com:stormasm/nomsleveldb.git

    In order to get everything to work you must do at least 2 things

    1. cd go/chunks; go get
    2. cd cmd/noms; go get

    Now everything should work including the noms binary and the chunk testing...
