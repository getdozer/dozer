export default async function () {
    const response = await fetch("https://github.com/getdozer/dozer/commits");
    const json = await response.json();
    return json;
}
