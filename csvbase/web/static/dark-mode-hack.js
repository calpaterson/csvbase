// FIXME: really should not need javascript for this
if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches){
    document.querySelector('html').setAttribute('data-bs-theme', 'dark');
    document.getElementById('codehilite-stylesheet').href = "/static/codehilite-dark.css";
}
