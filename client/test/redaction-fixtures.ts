export function credentialUrl(
  username: string,
  password: string,
  host: string,
  path = "",
): string {
  const url = new URL(`https://${host}${path}`);
  url.username = username;
  url.password = password;
  return url.toString();
}
