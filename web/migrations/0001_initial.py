# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.utils.timezone
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        ('auth', '0001_initial'),
        ('sites', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='User',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('password', models.CharField(max_length=128, verbose_name='password')),
                ('last_login', models.DateTimeField(default=django.utils.timezone.now, verbose_name='last login')),
                ('is_superuser', models.BooleanField(default=False, help_text='Designates that this user has all permissions without explicitly assigning them.', verbose_name='superuser status')),
                ('email', models.EmailField(unique=True, max_length=128, verbose_name='email address')),
                ('first_name', models.CharField(max_length=32, verbose_name='first name')),
                ('last_name', models.CharField(max_length=32, verbose_name='last name')),
                ('phone_number', models.CharField(max_length=15, verbose_name='phone_number', blank=True)),
                ('is_staff', models.BooleanField(default=False, help_text='Designates whether the user can loginto this admin site.', verbose_name='staff status')),
                ('is_manual_request_dispatcher', models.BooleanField(default=False, help_text='Designates whether the user can receive manual request nofications.', verbose_name='manual request dispatcher status')),
                ('is_active', models.BooleanField(default=True, help_text='Designates whether this user should be treated as active. Unselect this instead of deleting accounts.', verbose_name='active')),
                ('date_joined', models.DateTimeField(auto_now_add=True, verbose_name='date joined')),
                ('date_change_password', models.DateTimeField(auto_now_add=True, verbose_name='date change password')),
                ('groups', models.ManyToManyField(related_query_name='user', related_name='user_set', to='auth.Group', blank=True, help_text='The groups this user belongs to. A user will get all permissions granted to each of his/her group.', verbose_name='groups')),
                ('user_permissions', models.ManyToManyField(related_query_name='user', related_name='user_set', to='auth.Permission', blank=True, help_text='Specific permissions for this user.', verbose_name='user permissions')),
            ],
            options={
                'verbose_name': 'user',
                'verbose_name_plural': 'users',
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='ClassRange',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('class_from', models.IntegerField(null=True, blank=True)),
                ('class_to', models.IntegerField(null=True, blank=True)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Concept',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('key', models.CharField(max_length=128, verbose_name='key')),
                ('concept', models.CharField(max_length=500, null=True, verbose_name='concept')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='CustomSite',
            fields=[
                ('site_ptr', models.OneToOneField(parent_link=True, auto_created=True, primary_key=True, serialize=False, to='sites.Site', on_delete=models.CASCADE)),
                ('in_manutenzione', models.BooleanField(default=False)),
                ('label', models.CharField(max_length=255)),
            ],
            options={
                'verbose_name': 'Sito in manutenzione',
                'verbose_name_plural': 'Sito in manutenzione',
            },
            bases=('sites.site',),
        ),
        migrations.CreateModel(
            name='ExecutedQueryLog',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('query_title', models.CharField(max_length=255, verbose_name='Title')),
                ('query_body', models.CharField(max_length=20000, verbose_name='Body')),
                ('executed_by', models.IntegerField()),
                ('executed_at', models.DateTimeField(auto_now_add=True)),
                ('ip_address', models.CharField(max_length=15, verbose_name='IP')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='External_Metadata',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('table_name', models.CharField(max_length=128, verbose_name='table name')),
                ('column_name', models.CharField(max_length=128, null=True, verbose_name='column name')),
                ('id_value', models.CharField(max_length=10, null=True, verbose_name='column name')),
                ('key', models.CharField(max_length=128, verbose_name='key')),
                ('value', models.CharField(max_length=1500, verbose_name='value')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='ManualRequest',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('dispatcher', models.CharField(max_length=30, blank=True)),
                ('request_date', models.DateTimeField(auto_now_add=True)),
                ('dispatch_date', models.DateTimeField(null=True, blank=True)),
                ('dispatch_note', models.CharField(max_length=512, null=True, blank=True)),
                ('subject', models.CharField(max_length=512, verbose_name='subject')),
                ('goal', models.CharField(max_length=256, verbose_name='goal')),
                ('topic', models.CharField(max_length=100, verbose_name='topic')),
                ('requested_data', models.CharField(max_length=2048, verbose_name='requested data')),
                ('references_years', models.CharField(max_length=30, verbose_name='referenced years')),
                ('territorial_level', models.CharField(max_length=30, verbose_name='territorial level')),
                ('other_territorial_level', models.CharField(max_length=30, verbose_name='other territorial level (specify)', blank=True)),
                ('specific_territorial_level', models.CharField(max_length=400, verbose_name='specific territorial level', blank=True)),
                ('url', models.CharField(max_length=256, verbose_name='url')),
                ('dispatched', models.BooleanField(default=False)),
                ('inquirer', models.ForeignKey(to=settings.AUTH_USER_MODEL, null=True, on_delete=models.CASCADE)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Metadata',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('table_name', models.CharField(max_length=30, verbose_name='table name')),
                ('column_name', models.CharField(max_length=30, null=True, verbose_name='column name')),
                ('key', models.CharField(max_length=256, verbose_name='key')),
                ('value', models.CharField(max_length=256, verbose_name='value')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='OntologyFileModel',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=255)),
                ('upload', models.FileField(upload_to=b'ontologies')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Reconciliation',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('table_name', models.CharField(max_length=256, blank=True)),
                ('column_name', models.CharField(max_length=256, blank=True)),
                ('code_id', models.IntegerField()),
                ('url', models.CharField(max_length=512, blank=True)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Synonym',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('synonyms_list', models.CharField(max_length=5000)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='TerritorialLevel',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=255)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Test3',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('id1', models.IntegerField()),
                ('id2', models.IntegerField()),
                ('numerosity', models.IntegerField(verbose_name='numerosity')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Test4',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('id1', models.IntegerField()),
                ('id2', models.IntegerField()),
                ('id3', models.IntegerField()),
                ('numerosity', models.IntegerField(verbose_name='numerosity')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Test5',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('id1', models.IntegerField()),
                ('id2', models.IntegerField()),
                ('id3', models.IntegerField()),
                ('id4', models.IntegerField()),
                ('numerosity', models.IntegerField(verbose_name='numerosity')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='UserType',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=128, verbose_name='user type')),
                ('position', models.IntegerField()),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.AddField(
            model_name='user',
            name='user_type',
            field=models.ForeignKey(verbose_name='User type', to='web.UserType', null=True, on_delete=models.CASCADE),
            preserve_default=True,
        ),
    ]
