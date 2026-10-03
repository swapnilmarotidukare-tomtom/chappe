from chappe.transports.slack.formatter import SlackFormatter

fmt = SlackFormatter()


def test_escapes_slack_control_characters() -> None:
    assert fmt.escape("a < b & c > d") == "a &lt; b &amp; c &gt; d"
    assert fmt.bold("<x>") == "*&lt;x&gt;*"


def test_link_escapes_label_and_url_separators() -> None:
    expected = "<https://a.example/x%7Cy%3Ez|Run &lt;1&gt;>"
    assert fmt.link("https://a.example/x|y>z", "Run <1>") == expected


def test_mentions_and_icons_pass_through() -> None:
    assert fmt.mention("<!subteam^S0123>") == "<!subteam^S0123>"
    assert fmt.icon(":x:") == ":x:"
