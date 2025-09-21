const mockedData = [
    'https://en.wikipedia.org/wiki/New_York_City:title:New York City - Wikipedia',
    'https://www.nyc.gov/:title:Welcome to NYC.gov | City of New York',
    'https://www.nyctourism.com/:title:NYC Tourism + Conventions | Explore the Best Things to Do in NYC',
    'https://www.ny.gov/:title:The Official Website of New York State',
    'https://www.britannica.com/place/New-York-state:title:New York | Capital, Map, Population, History, &amp; Facts | Britannica',
    'https://nymag.com/:title:New York Magazine',
    'https://www.nytimes.com/:title:The New York Times - Breaking News, US News, World News and Videos',
    'https://en.wikipedia.org/wiki/New_York_(state):title:New York (state) - Wikipedia',
    'https://www.iloveny.com/:title:Explore New York Attractions &amp; Things To Do | Hotels &amp; Events'
];

let links;
let results;

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
    loadResults();
}

async function loadMore() {
    loadResults();
}

function makeLinkBox(url, title) {
    let box = "<div class=\"linkBox\">";
    box += '<a href=\"' + url + '\" class=\"linkTitle\">' + title + '</a>';
    box += '<a href=\"' + url + '\" class=\"resultLink\">' + url + '</a>';
    box += '</div>';
    return box;
}

/*async function getInfo(url) {
    const response = await fetch(url, {
        method: 'GET',
        mode: "cors",
    });
    console.log(response);
    if (!response.ok) {
        throw new Error('Network response was not ok');
    }
    const responseText = await response.text();
    const parsedResponse = (new window.DOMParser()).parseFromString(responseText, "text/html");
    const title = parsedResponse.title;
    const text = parsedResponse.getElementsByTagName("body")[0].textContent.slice(0, 100);
    return [title, text];
}*/

async function fetchResults() {
    /*const url = 'http://docsearch.cis5550.net:6877/search?terms=' + words;*/
    const url = 'https://lionelhu.cis5550.net/search?terms=' + words;
    const response = await fetch(url, {
        method: 'GET',
        mode: "cors",
    });
    /* console.log(response); */
    if (!response.ok) {
        throw new Error('Network response was not ok');
    }
    const resultsText = await response.text();
    if (resultsText === 'EMPTY') {
        results = [];
    } else {
        results = resultsText.split(':sep:');
    }
}

function loadResults() {
    if (results.length === 0) {
        document.getElementById('resultBox').innerHTML = 'No result';
        return;
    }

    nextIndex = Math.min(prevIndex + 10, results.length);
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