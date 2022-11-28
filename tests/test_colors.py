from folio_migration_tools.colors import Bcolors


def test_all_colors():
    assert Bcolors.HEADER == "\033[95m"
    assert Bcolors.OKBLUE == "\033[94m"
    assert Bcolors.OKCYAN == "\033[96m"
    assert Bcolors.OKGREEN == "\033[92m"
    assert Bcolors.WARNING == "\033[93m"
    assert Bcolors.FAIL == "\033[91m"
    assert Bcolors.ENDC == "\033[0m"
    assert Bcolors.BOLD == "\033[1m"
    assert Bcolors.UNDERLINE == "\033[4m"
