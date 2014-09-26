"""
Django jenkins settings for l4s project.
"""

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = '#=ubersecret!vz&f)@#(i'

# Database
# https://docs.djangoproject.com/en/1.6/ref/settings/#databases

DATABASES = {
     'default': {
          'ENGINE': 'django.db.backends.postgresql_psycopg2',
          'NAME': 'jenkins',
          'USER': 'jenkins',
        }
}

LOCAL_APPS = ('django_jenkins', )

PROJECT_APPS= ('web', )

JENKINS_TASKS = (
    'django_jenkins.tasks.with_coverage',
    #'django_jenkins.tasks.django_tests',   # select one django or
    #'django_jenkins.tasks.dir_tests'      # directory tests discovery
    'django_jenkins.tasks.run_pep8',
    'django_jenkins.tasks.run_pyflakes',
    #'django_jenkins.tasks.run_jslint',
    #'django_jenkins.tasks.run_csslint',
    'django_jenkins.tasks.run_sloccount',
    #'django_jenkins.tasks.lettuce_tests',
)

