# test_sample.py

def test_addition():
    assert 2 + 2 == 4

def test_subtraction():
    assert 5 - 3 == 2

class TestMultiplication:
    def test_multiply_positive_numbers(self):
        assert 2 * 3 == 6

    def test_multiply_negative_numbers(self):
        assert -4 * -5 == 20
