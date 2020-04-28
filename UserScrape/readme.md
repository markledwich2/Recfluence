# User Scrape

Python app that uses selenium to collect personalized recommendation data for prototypical users with history.

### Dev Setup

**Install Dependencies**
  - Open UserScrape (this folder) in vscode
  - `>RemoteContainers: Open Folder in Container`

or

  - Install recommended extensions (when prompted)
  - Install chromium
  - run pip3 install -r requirements.txt

**Create `userscrape.json`**

VsCode will provide intellisense/help populate this file. 

To use local storage emulation
  - Run the vscode command `Azurite: Start Blob Service`, or click Start BLob Emulator via `Azure (Panel) > STORAGE (section) > Attached Storage Accounts > Local Emulator > Blob Containers > Start Blob Emulator`)
  - Set data_storage_cs to 
```DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;```
