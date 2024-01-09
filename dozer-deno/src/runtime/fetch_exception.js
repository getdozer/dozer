export default async function () {
    const response = await fetch("https://github.com/getdozer/dozer");
    const json = await response.json();
    return json;
}
