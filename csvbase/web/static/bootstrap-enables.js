// This file is just for enabling various bootstrap features
// https://getbootstrap.com/docs/5.1/components/tooltips/#example-enable-tooltips-everywhere
var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
    return new bootstrap.Tooltip(tooltipTriggerEl)
});
