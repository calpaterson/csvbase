from csvbase import svc


from .utils import make_user, parse_form, current_user


def test_user_settings__cycle(sesh, client, test_user):
    with current_user(test_user):

        settings_url = f"/{test_user.username}/settings"
        get_resp = client.get(settings_url)
        assert get_resp.status_code == 200
        form = parse_form(get_resp.data)
        assert form.method == "POST"
        dict_form = dict(parse_form(get_resp.data))
        assert dict_form == {
            "timezone": "UTC",
            "email": "",
            "about": "",
        }

        new_timezone = "Asia/Tokyo"
        new_email = "example@example.com"
        new_about = "Hello, World"
        post_resp = client.post(
            settings_url,
            data={
                "timezone": new_timezone,
                "email": new_email,
                "mailing-list": "checked",
                "about": new_about,
                "use-gravatar": "checked",
            },
        )
        assert post_resp.status_code == 302, post_resp.data

        get_resp_2 = client.get(settings_url)
    new_form = dict(parse_form(get_resp_2.data))
    assert new_form == {
        "timezone": new_timezone,
        "email": new_email,
        "mailing-list": "on",
        "about": new_about,
        "use-gravatar": "on",
    }

    # finally, just double check it's in the db
    new_user_obj = svc.user_by_name(sesh, test_user.username)
    assert new_user_obj.settings.timezone == new_timezone
    assert new_user_obj.email == new_email
    assert new_user_obj.settings.mailing_list
    assert new_user_obj.settings.use_gravatar

    assert svc.get_user_bio_markdown(sesh, test_user.user_uuid) == new_about


def test_user_settings__updating_delete_email(sesh, client, test_user):
    with current_user(test_user):
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
    assert new_user_obj.settings.timezone == new_timezone
    assert new_user_obj.email is None


def test_nonsense_timezone(sesh, client, test_user):
    with current_user(test_user):
        settings_url = f"/{test_user.username}/settings"
        new_timezone = "MilkyWay/Moon"
        post_resp = client.post(settings_url, data={"timezone": new_timezone})
    assert post_resp.status_code == 400, post_resp.data

    new_user_obj = svc.user_by_name(sesh, test_user.username)
    assert new_user_obj.settings.timezone != "MilkyWay/Moon"


def test_user_settings__not_authed(client, test_user):
    settings_url = f"/{test_user.username}/settings"
    get_resp = client.get(settings_url)
    assert get_resp.status_code == 401


def test_user_settings__wrong_user(sesh, client, test_user, app):
    with current_user(test_user):
        user2 = make_user(sesh, app.config["CRYPT_CONTEXT"])

        settings_url = f"/{user2.username}/settings"
        get_resp = client.get(settings_url)
        assert get_resp.status_code == 401

        settings_url = f"/{user2.username}/settings"
        get_resp = client.post(settings_url, data={"timezone": "Europe/Berlin"})
    assert get_resp.status_code == 401


def test_change_password__happy(sesh, client, test_user):
    url = f"/{test_user.username}/settings/change-password"
    with current_user(test_user):
        get_resp = client.get(url)
        assert get_resp.status_code == 200
        form = dict(parse_form(get_resp.data))
        assert form == {
            "existing-password": "",
            "new-password": "",
            "new-password-again": "",
        }

        form["existing-password"] = "password"
        form["new-password"] = "password1"
        form["new-password-again"] = "password1"
        post_resp = client.post(url, data=form)
        assert post_resp.status_code == 302
        assert post_resp.headers["Location"] == f"/{test_user.username}"


def test_change_password__existing_password_is_wrong(sesh, client, test_user):
    url = f"/{test_user.username}/settings/change-password"
    with current_user(test_user):
        get_resp = client.get(url)
        assert get_resp.status_code == 200
        form = dict(parse_form(get_resp.data))
        assert form == {
            "existing-password": "",
            "new-password": "",
            "new-password-again": "",
        }

        form["existing-password"] = "pass word"
        form["new-password"] = "password1"
        form["new-password-again"] = "password1"
        post_resp = client.post(url, data=form)
        assert post_resp.status_code == 400


def test_change_password__new_passwords_dont_match(sesh, client, test_user):
    url = f"/{test_user.username}/settings/change-password"
    with current_user(test_user):
        get_resp = client.get(url)
        assert get_resp.status_code == 200
        form = dict(parse_form(get_resp.data))
        assert form == {
            "existing-password": "",
            "new-password": "",
            "new-password-again": "",
        }

        form["existing-password"] = "password"
        form["new-password"] = "password1"
        form["new-password-again"] = "password2"
        post_resp = client.post(url, data=form)
        assert post_resp.status_code == 400
