Getting a token
===============

The API requires a valid token. To register for a token,
sign up for a user account at http://api.watttime.org/accounts/register/.
Your API token will be available at http://api.watttime.org/accounts/token/.

The default token permissions aren't sufficient to allow access to
the marginal carbon emissions data, which the client library provides.
To request an account upgrade,
email the WattTime Dev team at <dev@watttime.org>
with a quick explanation of what you'll use the marginal carbon data for.

Once your account has been upgraded,
the last step is to set the token as an environment variable.
This is required for running the test suite,
and it's good practice anyway.
Type this line in your bash shell, or add it to your .bashrc,
replacing the dummy token string with your actual token::

   export WATTTIME_API_TOKEN=abcdef0123456abcdef0123456
