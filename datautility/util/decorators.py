''' Decorators '''


def noneable(type_converter):
    '''
    A decorater that converts a type converter utility function returning converted value,
    into a function returning it's corresponding function with parameter to specify
    if an empty value is to be converted into null or not.
    '''
    def wrapper(convert_null=True):
        ''' Higher order wrapper function for the type converter. '''
        if convert_null:
            return lambda x: None if x == '' else type_converter(x)

        return type_converter

    return wrapper
