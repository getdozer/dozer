export default async function () {
    const response = await fetch('https://api.github.com/repos/getdozer/dozer/commits?per_page=1');
    const json = await response.json();
    return json;
}
