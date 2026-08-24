# Getting this onto GitHub

This folder is already a git repo with one commit made. Two ways to publish it.

## Option A — browser only, no terminal

1. Go to github.com/new
2. Name it `rundown`, choose Private, and do NOT check any of the
   "Initialize with" boxes
3. Click Create repository
4. On the empty repo page, click **uploading an existing file**
5. Drag in `server.js`, `package.json`, `README.md`, and `.gitignore`
6. Click Commit changes

Done. Skip to "Connect Railway" below.

## Option B — terminal

Make an empty repo on github.com/new as above, then in this folder:

    git remote add origin https://github.com/YOURNAME/rundown.git
    git push -u origin main

The commit is already made, so that's all there is.

## Connect Railway

1. Railway dashboard -> New Project -> Deploy from GitHub repo
2. Pick `rundown`. It builds and starts on its own
3. In the same project: New -> Database -> Redis
4. Click your app service -> Variables -> Add Variable Reference -> REDIS_URL
   (it redeploys itself)
5. App service -> Settings -> Networking -> Generate Domain

## Check it worked

Open `https://your-domain/healthz` in a browser. You want:

    {"ok":true,"storage":"Redis","reachable":true}

If it says `memory`, step 4 did not attach. Redo it.

## Then

Pick a token, any word 4+ letters. Same word in both places.

Phone:   https://your-domain/phone?token=yourword
Glasses: https://your-domain/?token=yourword

Add the glasses one in the Meta AI app under
App Settings -> App Connections -> Web Apps -> Add a Web App.
