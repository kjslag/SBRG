all: SBRG BSBRG2 FSBRG2

profile: SBRGp BSBRG2p FSBRG2p

# GHC=stack ghc --
# GHCp=stack ghc --profile -- -rtsopts -prof

GHC=ghc
GHCp=ghc -rtsopts -prof

SBRG: SBRG.hs
	$(GHC) SBRG

BSBRG2: SBRG2.hs
	@[[ -L BSBRG2.hs ]] || ln -s SBRG2.hs BSBRG2.hs
	$(GHC) -DBOSONIC BSBRG2

FSBRG2: SBRG2.hs
	@[[ -L FSBRG2.hs ]] || ln -s SBRG2.hs FSBRG2.hs
	$(GHC) -DFERMIONIC FSBRG2

SBRGp: SBRGp.hs
	@[[ -L SBRGp.hs ]] || ln -s SBRG.hs BSBRG2p.hs
	$(GHCp) SBRGp

BSBRG2p: SBRG2.hs
	@[[ -L BSBRG2p.hs ]] || ln -s SBRG2.hs BSBRG2p.hs
	$(GHCp) -DBOSONIC BSBRG2p

FSBRG2p: SBRG2.hs
	@[[ -L FSBRG2p.hs ]] || ln -s SBRG2.hs FSBRG2p.hs
	$(GHCp) -DFERMIONIC FSBRG2p
