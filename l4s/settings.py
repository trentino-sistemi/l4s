# This file is part of Lod4Stat.
# This file is part of Lod4Stat.
#
# Copyright (C) 2014 Provincia autonoma di Trento
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
Django settings for l4s project.

For more information on this file, see
https://docs.djangoproject.com/en/1.6/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.6/ref/settings/
"""

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
import os
import locale

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
locale.setlocale(locale.LC_ALL, '')

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.6/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'VeryLongSecret'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

SENDER = "SSPAT"
SENDER_NAME = "Servizio Statistica: Provincia Autonoma di Trento"
PASSWORD_DURATION_DAYS = 90
PRIVACY_POLICY_PDF = "InformativaPrivacyGenericaISPAT_2018.pdf"

ALLOWED_HOSTS = ['www.l4s.ispat.provincia.tn.it', '*']

AUTHENTICATION_BACKENDS = (
    # Needed to login by username in Django admin, regardless of `allauth`
    "django.contrib.auth.backends.ModelBackend",
    # `allauth` specific authentication methods, such as login by e-mail
    "allauth.account.auth_backends.AuthenticationBackend"
)

LOGIN_URL = '/accounts/login'
LOGIN_REDIRECT_URL = '/'

# Application definition
INSTALLED_APPS = (
    'django_admin_bootstrapped',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.sites',
    # THIRD PARTY APPS
    'allauth',
    'allauth.account',
    'explorer',
    # CUSTOM APPS
    'web',
    'jsonify',
)

MIDDLEWARE = (
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
)

ACCOUNT_ACTIVATION_DAYS = 1

AUTH_USER_MODEL = 'web.User'

# Allauth settings
ACCOUNT_SIGNUP_FORM_CLASS = 'web.forms.SignupForm'
ACCOUNT_AUTHENTICATION_METHOD = "email"
ACCOUNT_EMAIL_REQUIRED = True
ACCOUNT_USERNAME_REQUIRED = False
ACCOUNT_USER_MODEL_USERNAME_FIELD = None
ACCOUNT_EMAIL_VERIFICATION = True
ACCOUNT_CONFIRM_EMAIL_ON_GET = True
ACCOUNT_LOGIN_ON_EMAIL_CONFIRMATION = False
ACCOUNT_EMAIL_CONFIRMATION_ANONYMOUS_REDIRECT_URL = '/accounts/success'

# Email settings for sending accout activation mails
DEFAULT_FROM_EMAIL = 'l4s@example.com'
ADMINISTRATOR_EMAIL = ['m.voltolini@trentinosistemi.com', 'ivan.benuzzi@provincia.tn.it', 'simone.ziglio@provincia.tn.it', 'arianna.demozzi@provincia.tn.it']
#ADMINISTRATOR_EMAIL = ['m.voltolini@trentinosistemi.com']
EMAIL_USE_TLS = True
EMAIL_HOST = "example.com"
EMAIL_HOST_USER = "smtp@example.com"
EMAIL_HOST_PASSWORD = "password"
EMAIL_PORT = 25

ROOT_URLCONF = 'l4s.urls'

WSGI_APPLICATION = 'l4s.wsgi.application'

# Database
# https://docs.djangoproject.com/en/1.6/ref/settings/#databases

EXPLORER_CONNECTIONS = {'lod4stat': 'lod4stat'}
EXPLORER_DEFAULT_CONNECTION = 'lod4stat'
EXPLORER_CONNECTION_NAME = 'lod4stat'
EXPLORER_PERMISSION_VIEW = lambda u: True
EXPLORER_PERMISSION_CHANGE = lambda u: True
EXPLORER_RECENT_QUERY_COUNT = 10
EXPLORER_DEFAULT_ROWS = 20
EXPLORER_DEFAULT_COLS = 10
EXPLORER_LOGIN_URL = LOGIN_URL

# Subject to discriminate column that contains descriptions.
DESCRIPTION_SUBJECT = 'http://it.dbpedia.org/data/Descrizione'

DATABASES = {
        # Django database.
        'default': {
            'ENGINE': 'django.db.backends.postgresql_psycopg2',
            'NAME': 'djangodb',
            'USER': 'l4s',
            'PASSWORD': 'django',
            'HOST': 'db',
            'PORT': '', },
        # Main database used to perform the queries.
        'lod4stat': {
            'ENGINE': 'django.db.backends.postgresql_psycopg2',
            'NAME': 'lod4stat',
            'USER': 'l4s',
            'PASSWORD': 'django',
            'HOST': 'db',
            'PORT': '', },
        'source': {
            'ENGINE': "sql_server.pyodbc",
            'ENGINE': "mssql",
            'HOST': "mssql",
            'USER': "sa",
            'PASSWORD': "passwordL4S",
            'NAME': "DATI",
            'OPTIONS': {
                'driver': 'ODBC Driver 17 for SQL Server',
            },
        },
    }

DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# In a Windows environment this must be set to your system time zone.
TIME_ZONE = 'Europe/Rome'

# Internationalization
# https://docs.djangoproject.com/en/1.6/topics/i18n/

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'it-IT'

SITE_ID = 1

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = True

# If you set this to False, Django will not format dates, numbers and
# calendars according to the current locale.
USE_L10N = True

# If you set this to False, Django will not use timezone-aware datetimes.
USE_TZ = True

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.6/howto/static-files/

STATIC_URL = '/static/'


# List of finder classes that know how to find static files in
# various locations.
STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
    #'django.contrib.staticfiles.finders.DefaultStorageFinder',
)

# sudo (2018-03-27): runserver, find static files when DEBUG = True
STATICFILES_DIRS = (
    os.path.join(BASE_DIR, "l4s/static"),
)

# Template configuration
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            os.path.join(os.path.dirname(os.path.realpath(__file__)), "static",
                         "templates"),
        ],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
            'debug': True,
        },
    },
]

# sudo (2018-03-27): STATIC_ROOT is where collectstatic writes
#                    django.views.static.serve looks there (DEBUG = False)
#                    webserver needs to look here
# STATIC_ROOT = os.path.join(BASE_DIR, "l4s/static")  # 2018-03-27, wrong
STATIC_ROOT = os.path.join(BASE_DIR, "static")

LOCALE_PATHS = ('conf/locale',)

CONTENT_TYPES = ['application/rdf+xml']

LEGEND = "* = dato coperto da segreto statistico"
DL_ART = "art. 9 D.L. 322/89"
LINK_DL_ART = "https://www.istat.it/it/files/2011/04/dlgs322.pdf"

LOG_TIMEFMT = '%Y-%m-%d %H:%M:%S %z'
LOG_TIMEFMT_SIMPLE = '%d %b %H:%M:%S'
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format':
            "[%(asctime)s] %(levelname).3s [%(name)s:%(lineno)s] %(message)s",
            'datefmt': LOG_TIMEFMT
        },
        'simple': {
            'format': '%(asctime)s %(levelname).3s: %(message)s',
            'datefmt': LOG_TIMEFMT_SIMPLE
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
            'stream': 'ext://sys.stdout'
        },
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': 'INFO'
        },
    }
}

# Only defined in settings_local.py.
LOCAL_APPS = ()
try:
    from settings_local import *
    INSTALLED_APPS += LOCAL_APPS
except ImportError:
    pass
