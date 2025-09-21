let links;

const urlParams = new URLSearchParams(window.location.search);
const words = urlParams.get('words');

let nextIndex = 10;
let prevIndex = 0;

window.addEventListener('scroll', () => {
    if (
        window.scrollY + window.innerHeight >= document.body.offsetHeight - 1000
    ) {
        loadMore();
    }
});

async function load() {
    links = '';
    document.getElementById("searchInput").value = words.split(':SEP:').join(' ');
    await fetchResults();
}

async function loadMore() {
    nextIndex += 10;
    await fetchResults();
}

function makeLinkBox(url, title) {
    let box = "<div class=\"linkBox\">";
    box += '<a href=\"' + url + '\" class=\"linkTitle\">' + title + '</a>';
    box += '<a href=\"' + url + '\" class=\"resultLink\">' + url + '</a>';
    box += '</div>';
    return box;
}

async function fetchResults() {
    const url = 'https://3.208.106.101/search?terms=' + words + '&start=1&end=' + nextIndex.toString();
    const response = await fetch(url, {
        method: 'GET',
        mode: "cors",
    });
    console.log(response);
    if (!response.ok) {
        throw new Error('Network response was not ok');
    }
    const resultsText = await response.text();
    console.log(resultsText);
    const results = resultsText.split(':sep:');
    nextIndex = results.length;
    for (let i = prevIndex; i < nextIndex; i++) {
        const [url, title] = results[i].split(':title:');
        const linkBox = makeLinkBox(url, title);
        links += linkBox;
    }
    prevIndex = nextIndex;
    document.getElementById('resultBox').innerHTML = links;
}

function handleSearch() {
    const words = document.getElementById('searchInput').value.split(' ');
    const result = words.filter((word) => word.length !== 0).join(':SEP:');
    window.location.href = "../result/result.html?words=" + result;
}