# BitTorrent Client
Simple implementation of BitTorrent Client.

### Requirements
```
aiohttp==2.3.10
bitarray==0.8.3
```
### Usage
1. Start a server
    ```sh
    $ python main.py start
    ```
2. Send request via new cmd (implemented command below) 
    ```
    'start'  - Start a server, 
    'show'   - Preview torrent files,
    'add'    - Add a new torrent for downloading,
    'pause'  - Set a pause,
    'resume' - Renew torrent downloading, 
    'remove' - Remove torrent from downloading, 
    'status' - Stats about progress and so on.
    ```
## Author

| [<img src="https://avatars3.githubusercontent.com/u/19955305?s=460&v=4" width="100px;"/><br /><sub><b>Roman Budlyanskiy </b></sub>](https://github.com/bully434)<br /> |
|---|