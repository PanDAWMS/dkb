function showStuff(id) {
    var x = document.getElementById('taskname_'+id);
    var y = document.getElementById('total_'+id);
    if (y.style.display == 'none' || y.style.display == '') {
        y.style.display = 'block';
    } else {
        y.style.display = 'none';
    }
}