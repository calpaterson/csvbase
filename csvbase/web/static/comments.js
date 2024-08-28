document.addEventListener('DOMContentLoaded', () => {
    const commentTextareas = document.querySelectorAll('.comment-textarea');

    commentTextareas.forEach(textarea => {
        // Make the textarea fit the text inside.  CSS will be able to do this
        // soon with content-sizing: fixed
        textarea.style.height = ""; // Reset any existing height styles
        textarea.style.height = (textarea.scrollHeight + 5) + "px";
    });


});
