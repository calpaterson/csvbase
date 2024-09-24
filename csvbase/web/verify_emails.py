import base64
from email.message import EmailMessage

from werkzeug.wrappers.response import Response
from flask import render_template, request, url_for, Blueprint, redirect, make_response

from csvbase import email, svc
from csvbase.sesh import get_sesh
from csvbase.web.func import get_current_user_or_401

bp = Blueprint("verify_emails", __name__)


@bp.route("/verify-email", methods=["GET", "POST"])
def send_verification_email() -> Response:
    sesh = get_sesh()
    current_user = get_current_user_or_401()
    email_address = current_user.email
    if email_address is None:
        raise RuntimeError("no email set")
    if request.method == "POST":
        verification_code: bytes = svc.generate_email_verification_code(
            sesh, current_user.user_uuid
        )
        urlsafe_code: str = base64.urlsafe_b64encode(verification_code).decode("utf-8")

        verification_url = url_for(
            "verify_emails.verify_email",
            urlsafe_email_verification_code=urlsafe_code,
            _external=True,
        )

        em = EmailMessage()
        em.set_content(
            render_template(
                "email/verify-email.txt",
                verification_url=verification_url,
                user=current_user,
            )
        )
        em["Subject"] = "Verify your email address"
        em["To"] = email_address
        em["From"] = f"csvbase@{request.host}"
        message_id = f"<verify-email-{urlsafe_code}@{request.host}>"
        em["Message-ID"] = message_id

        email.validate(em)
        sesh.commit()
        email.send(em)
        return redirect(url_for("verify_emails.send_verification_email"))
    else:
        return make_response(
            render_template("email-verification-sent.html", user=current_user)
        )


@bp.route("/verify-email/<urlsafe_email_verification_code>", methods=["GET"])
def verify_email(urlsafe_email_verification_code: str) -> Response:
    sesh = get_sesh()
    current_user = get_current_user_or_401()
    email_verification_code = base64.urlsafe_b64decode(
        urlsafe_email_verification_code.encode("utf-8")
    )
    svc.verify_email(sesh, current_user.user_uuid, email_verification_code)
    sesh.commit()
    return make_response(render_template("email-verified.html"))
