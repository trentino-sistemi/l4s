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
Django tests for l4s project.
"""

from django.test import LiveServerTestCase, TestCase
from django.contrib.auth import get_user_model
from mechanize import Browser


class UserTest(TestCase):
    """
    Test the new user creation.
    """
    def test_create_user(self):
        """
        Create a new user.
        """
        User = get_user_model()
        test_user = User.objects.create_user(email='test@email.com',
                                             first_name='Name',
                                             last_name='Surname',
                                             phone_number='02666666',
                                             password='test')
        test_user.delete()


class MechanizeTestCase(LiveServerTestCase):

    def setUp(self):
        """
        Open a browser on a virtual X server and show the initial page.
        """
        self.br = Browser()
        if self.initial_page:
            self.br.open(self.live_server_url + self.initial_page)


class RegisterTest(MechanizeTestCase):
    """
    Test that the signup page is available and it has some required fields.
    """
    def setUp(self):
        """
        Go to the signup page.
        """
        self.initial_page = '/accounts/signup/'
        super(RegisterTest, self).setUp()

    def test_register_page(self):
        """
        Test the registration page.
        """
        self.assertTrue(self.br.viewing_html())

    def test_register_fields(self):
        """
        Test the required fields.
        """
        self.br.select_form(nr=0)
        for required_field in ('email', 'password1', 'password2'):
            self.assertIsNotNone(self.br[required_field])


class AdminTest(MechanizeTestCase):
    """
    Test that the admin page is available.
    """
    def setUp(self):
        """
        Go to the admin page.
        """

        self.initial_page = '/admin/'
        super(AdminTest, self).setUp()

    def test_admin_page(self):
        """
        Assert the page availability-
        """
        self.assertTrue(self.br.viewing_html())
