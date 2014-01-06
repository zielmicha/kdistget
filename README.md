kdistget
========

Really simple distributed file download/synchronization program.

Usage
---------

Start tracker.

    python tracker.py 7777
    
Start at least one seed, generating .kdg file (replace port 5001 with port you wish clients to communicate on)

    python client.py 5001 --seed path/to/file/to/distribute localhost:7777
    
Start downloader:

    python client.py 5002 path/to/file/to/download path/to/file/to/distribute.kdg localhost:7777
