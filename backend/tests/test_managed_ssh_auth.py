import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path", "json_body"),
    [
        ("GET", "/api/ssh-profiles", None),
        ("GET", "/api/tasks/1/ssh-access", None),
        (
            "POST",
            "/api/files/ssh/download",
            {
                "host": "example.invalid",
                "username": "nobody",
                "path": "/tmp/nope",
            },
        ),
    ],
)
async def test_managed_ssh_is_unavailable_when_auth_token_is_empty(
    client,
    method,
    path,
    json_body,
):
    response = await client.request(method, path, json=json_body)

    assert response.status_code == 503
    assert "AUTH_TOKEN" in response.text
