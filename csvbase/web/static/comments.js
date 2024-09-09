document.addEventListener('DOMContentLoaded', () => {
    const commentTextarea = document.querySelector('#comment-textarea');

    if (commentTextarea.value.trim() != ""){
        // Make the textarea fit the text inside.  CSS will be able to do this
        // soon with content-sizing: fixed
        commentTextarea.style.height = ""; // Reset any existing height styles
        commentTextarea.style.height = (commentTextarea.scrollHeight + 5) + "px";
        commentTextarea.setSelectionRange(commentTextarea.value.length, commentTextarea.value.length);
    }
});
