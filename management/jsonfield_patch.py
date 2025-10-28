"""
JSONField compatibility patch for social_django with Django 3.2+
This patch fixes the missing 'context' argument in from_db_value method.
"""

import json
import six
import functools
from django.core.exceptions import ValidationError
from django.conf import settings
from django.db import models
try:
    # Django <4
    from django.utils.encoding import smart_text as smart_str_compat
except ImportError:
    # Django >=4 uses smart_str/force_str
    from django.utils.encoding import smart_str as smart_str_compat
from social_core.utils import setting_name

# Determine field metaclass based on Django version
if hasattr(models, 'SubfieldBase'):
    field_metaclass = models.SubfieldBase
else:
    field_metaclass = type

field_class = functools.partial(six.with_metaclass, field_metaclass)

if getattr(settings, setting_name('POSTGRES_JSONFIELD'), False):
    from django.contrib.postgres.fields import JSONField as JSONFieldBase
else:
    JSONFieldBase = field_class(models.TextField)


class JSONField(JSONFieldBase):
    """
    Patched JSONField that handles Django 3.2+ compatibility
    by properly implementing from_db_value with context parameter.
    """

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('default', dict)
        super(JSONField, self).__init__(*args, **kwargs)

    def from_db_value(self, value, expression, connection, context=None):
        """
        Fixed from_db_value method that accepts context parameter
        for Django 3.2+ compatibility.
        """
        return self.to_python(value)

    def to_python(self, value):
        """
        Convert the input JSON value into python structures, raises
        django.core.exceptions.ValidationError if the data can't be converted.
        """
        if self.blank and not value:
            return {}
        value = value or '{}'
        if isinstance(value, six.binary_type):
            value = six.text_type(value, 'utf-8')
        if isinstance(value, six.string_types):
            try:
                # with django 1.6 i have '"{}"' as default value here
                if value[0] == value[-1] == '"':
                    value = value[1:-1]

                return json.loads(value)
            except Exception as err:
                raise ValidationError(str(err))
        else:
            return value

    def validate(self, value, model_instance):
        """Check value is a valid JSON string, raise ValidationError on
        error."""
        if isinstance(value, six.string_types):
            super(JSONField, self).validate(value, model_instance)
            try:
                json.loads(value)
            except Exception as err:
                raise ValidationError(str(err))

    def get_prep_value(self, value):
        """Convert value to JSON string before save"""
        try:
            return json.dumps(value)
        except Exception as err:
            raise ValidationError(str(err))

    def value_to_string(self, obj):
        """Return value from object converted to string properly"""
        return smart_str_compat(self.value_from_object(obj))

    def value_from_object(self, obj):
        """Return value dumped to string."""
        orig_val = super(JSONField, self).value_from_object(obj)
        return self.get_prep_value(orig_val)


def patch_social_django_jsonfield():
    """
    Monkey patch social_django's JSONField to fix Django 3.2+ compatibility
    """
    import social_django.fields
    social_django.fields.JSONField = JSONField
    print("[PATCH] Applied social_django JSONField compatibility patch for Django 3.2+")