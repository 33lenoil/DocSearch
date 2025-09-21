function handleSearch() {
    const words = document.getElementById('searchInput').value.split(' ');
    const result = words.filter((word) => word.length !== 0).join(':SEP:');
    window.location.href = "../result/result.html?words=" + result;
}