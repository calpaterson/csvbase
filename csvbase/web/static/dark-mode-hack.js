// Bootstrap does not currently have a way to do this automatically so custom JS required
if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches){
    document.querySelector('html').setAttribute('data-bs-theme', 'dark');
    document.getElementById('codehilite-stylesheet').href = "/static/codehilite-dark.css";
}
