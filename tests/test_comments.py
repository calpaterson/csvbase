from csvbase import comments


def test_create_slug(sesh):
    title = "What does null mean in this context?"
    slug = comments._create_thread_slug(sesh, title)
    assert slug.startswith("what-does-null")
