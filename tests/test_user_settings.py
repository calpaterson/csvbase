from csvbase.web.func import set_current_user
from csvbase import svc


from .utils import make_user, parse_form


def test_user_settings__cycle(sesh, client, test_user):
    set_current_user(test_user)

    settings_url = f"/{test_user.username}/settings"
    get_resp = client.get(settings_url)
    assert get_resp.status_code == 200
    form = parse_form(get_resp.data)
    assert form.method == "POST"
    dict_form = dict(parse_form(get_resp.data))
    assert dict_form == {
        "timezone": "UTC",
        "email": "",
    }

    new_timezone = "Asia/Tokyo"
    new_email = "example@example.com"
    post_resp = client.post(
        settings_url,
        data={"timezone": new_timezone, "email": new_email, "mailing-list": "checked"},
    )
    assert post_resp.status_code == 302, post_resp.data

    get_resp_2 = client.get(settings_url)
    new_form = dict(parse_form(get_resp_2.data))
    assert new_form == {
        "timezone": new_timezone,
        "email": new_email,
        "mailing-list": "on",
    }

    # finally, just double check it's in the db
    new_user_obj = svc.user_by_name(sesh, test_user.username)
    assert new_user_obj.timezone == new_timezone
    assert new_user_obj.email == new_email
    assert new_user_obj.mailing_list


def test_user_settings__updating_delete_email(sesh, client, test_user):
    set_current_user(test_user)

    settings_url = f"/{test_user.username}/settings"
    get_resp = client.get(settings_url)
    assert get_resp.status_code == 200

    new_timezone = "Asia/Tokyo"
    new_email = ""
    post_resp = client.post(
        settings_url, data={"timezone": new_timezone, "email": new_email}
    )
    assert post_resp.status_code == 302, post_resp.data

    new_user_obj = svc.user_by_name(sesh, test_user.username)
    assert new_user_obj.timezone == new_timezone
    assert new_user_obj.email is None


def test_nonsense_timezone(sesh, client, test_user):
    set_current_user(test_user)

    settings_url = f"/{test_user.username}/settings"
    new_timezone = "MilkyWay/Moon"
    post_resp = client.post(settings_url, data={"timezone": new_timezone})
    assert post_resp.status_code == 400, post_resp.data

    new_user_obj = svc.user_by_name(sesh, test_user.username)
    assert new_user_obj.timezone != "MilkyWay/Moon"


def test_user_settings__not_authed(client, test_user):
    settings_url = f"/{test_user.username}/settings"
    get_resp = client.get(settings_url)
    assert get_resp.status_code == 401


def test_user_settings__wrong_user(sesh, client, test_user, app):
    set_current_user(test_user)
    user2 = make_user(sesh, app.config["CRYPT_CONTEXT"])

    settings_url = f"/{user2.username}/settings"
    get_resp = client.get(settings_url)
    assert get_resp.status_code == 401

    settings_url = f"/{user2.username}/settings"
    get_resp = client.post(settings_url, data={"timezone": "Europe/Berlin"})
    assert get_resp.status_code == 401
