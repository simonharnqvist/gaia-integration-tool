from lsdb import Catalogue

def crossmatch_with_gaia(gaia_catalogue: Catalogue, other_catalogue: Catalogue) -> Catalogue:
    xmatch = gaia_catalogue.crossmatch(other_catalogue)
    return xmatch