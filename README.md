# Rundown

Send YouTube videos from your phone, watch them on Meta Ray-Ban Display glasses.

Two files. No dependencies, no npm install, no build step.

## Deploy to Railway

1. Put `server.js` and `package.json` in a folder
2. `railway init` then `railway up` (or point Railway at a GitHub repo)
3. Add storage: **New -> Database -> Redis**, then your app service ->
   **Variables -> Add Variable Reference -> REDIS_URL**
4. **Settings -> Networking -> Generate Domain**

Check it worked: open `https://your-domain/healthz`. It should say
`"storage":"Redis"` and `"reachable":true`.

Without Redis it still runs, but the queue empties on restart.

## Use it

Pick any word as your token, 4+ letters. Same word both places.

- **Glasses:** add `https://your-domain/?token=yourword` in the Meta AI app
  under App Settings -> App Connections -> Web Apps -> Add a Web App
- **Phone:** open `https://your-domain/phone?token=yourword`

## Gestures

Rundown: swipe up/down to move the cue, index pinch to play.

Player: pinch play/pause, left/right skip 10s, up/down volume,
middle pinch back to the list.

## Known limits

- Some videos block embedding and will not play. No workaround.
- The player is always signed out, so ads play even with Premium.
- Anyone with your token can read and write your queue.
