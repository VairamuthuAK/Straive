import re
import json
import scrapy
from ..utils import *
from inline_requests import inline_requests


def xpath_literal(s):

    """
    Safely converts a Python string into a valid XPath string literal.

    Logic:
    - If the string contains no double quotes, it is wrapped in double quotes.
    - If the string contains no single quotes, it is wrapped in single quotes.
    - If the string contains both, an XPath concat() expression is generated.

    """

    if '"' not in s:
        return f'"{s}"'
    if "'" not in s:
        return f"'{s}'"
    return "concat(" + ", ".join(f'"{p}"' for p in s.split('"')) + ")"


def safe_int(value):
    """
    Safely extracts the first integer value from a string.

    This utility function is useful when numeric values are embedded
    within text (e.g., "Seats Still Available: 12") and need to be
    parsed without raising exceptions.
    """

    if not value:
        return None
    m = re.search(r'\d+', value)
    return int(m.group()) if m else None


MONTH_ABBR_RE = re.compile(
    r"-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$",
    re.I
)

def normalize_date(raw_date):
    """
    Converts:
      02-May   -> 2
      2-May    -> 2
      02       -> 2
      2-5      -> 2-5
      5 & 6    -> 5 & 6
    """
    if not raw_date:
        return raw_date

    # Remove month suffix like -May
    raw_date = MONTH_ABBR_RE.sub("", raw_date)

    # Remove leading zeros
    raw_date = re.sub(r"\b0+(\d)", r"\1", raw_date)

    return raw_date.strip()


class WichitaSpider(scrapy.Spider):
    name="wich"
    institution_id = 258432103505487824

    course_url = "https://webapps.wichita.edu/CourseSearch/CourseSearch"
    course_payload = "ctl00%24MainContent%24ScriptManager=ctl00%24MainContent%24UpdatePanel%7Cctl00%24MainContent%24SubjectCbl%240&__EVENTTARGET=ctl00%24MainContent%24SubjectCbl%240&__EVENTARGUMENT=&__LASTFOCUS=&__VIEWSTATE=bGX4xlq1yTQnrGiCg4vLTp8GTqshb%2Fhw3X14R0zek2x2v%2BBe7bdlymisXCPerJgims%2FPykFqT20q9tfp%2BYGkgdDZU%2Bw69qvNwyjfWoA0JNa3fHz6WsVe4SxP9FRL%2B7NU1%2FN4l27rnGiMhb5wLlGfHXLtgpEUDA9Bme%2BFp9IFZA3eJHWB6P0DCcLyqZGfBgGjqOjAAfrxWK89UEiWcEVRTDA6V0%2Fbm0bw6NFNU4rC%2BrJ%2F%2FFNGnE9wuUJ3YS%2BFTn2JbgM4TP2pdBY%2FHxkIGtHX12CWbT2SIEJqhZv7wFYPV8pvfkrObtMmVru3sEjqNRnA6qg5L474Sfh7oJgTRyqrzBvBhNBF4PV9XWvtmAPIL1wb2LZ5SMUB%2FdBBzQu7zeJXzfK1y5fPnNOcbpm4o9cpj9mEuRQdxTnHCSZOTdLfSUVYb1kGoW5y9jj9JCq8XhhN2y%2FMrqe2Y6LhtQT7cp%2BreSq8tvqrJ1pAeLa4f7kdbYxuFdWOwhOvlrfkylJRv9NxaePkrc9OFEvHcwG51az6KlQza%2FAZ%2Fs%2F1zIp59%2Fj6NC6bFjnY4bCNX%2FvvX%2Bk44S6rsNIDYOxdxQzrsRz5ZMnacCFircxnMcL%2FcjrMyBkmQmlyivWzlSFtiBKjLp%2BuoRrAJXDLI0EwyJhsUIgEqLVHJRGgsoKrizwTI0pdWNLB7dC5Gx%2Fe4dCjkxkJTXrOqqJyDRVbnluY%2B8bJZOnN0txIzuc0Fap698m1MxeysNAc6DzRvQ6IOnfTrISFBdm002xrs02lo%2Bjvy8UYfVi7MCfqzyB587UvceXF2juFIkQSPY9XmGIIbDKIiDG3KsHa3JwUh9u0zKy40QkG6WoCbq5lDxXcw7uVhQRf646%2FDyFZM89EjwZIub20O3ke4Zad5WzBh2f4NSsoh9gSL8LqkTdJduSTHQVAW5hN1%2Fa0mbY5setVilk%2Fy38Hx6VN2CucBi6%2F2VBQjIuuyURTpeqdlcvWOd%2FbpYFNmSCGRgn1gYSUWgmGNc6z0uEwKfDxQcjvjAd73hUcN7CHNYTE9lkGAG7K7cSbfiLTzAqsXhU2bSp%2BZL1aN1cGrioBfbZwgHRD0E7xRCgUhEpZf1Jta1rKUrNa1szkUdZ1CSPQJghqNvAJw8NlGZwQNX9P49n3%2Fn%2FzTjbUa8BdjQ10vboBQhhuZCEUmoB7qm0amZOj7SSgBHjXZx4PL2RXKX%2Fz5mo4%2BUQ3OdHBJQ%2FYMFk9MwxvBrAO1LNAUqHTt9AmwLlaGKQC5YnfCcBmMRjA9Wx8QuPqbI0C1XssghA%2Fyis0hpCKEJ%2FS6Qyx2M8JuLfpNtL%2B0LPJJQFjmGxKxdrG%2BgSIWpEbaRwPFkHqV5dU0Daf1iwwsXvFyvy73HUTSFmIdn2DSCdmCSD%2Fl04wAwtmeA%2FJF0ULPR2xNJSkkAfg4PB3%2FwlPVTT0CuDKJCA9Ruq2whWwgqtPB%2Fcj%2FkOKvCjrsDuM16Kx5TeB8ptMAmy5PF2gxCPFskMhVBt5vCJKQia59ipQSKQvMHb3henlTO5NL0QpuSA7S3du2VAVlCHX34HlZmQeybNNdDiLWrekAG5LeS6EhncEq3OEHfB884pvE4aceQfXrThq%2BAnXReb%2FcD4K4NYqvt1bcjXA4qbzP9hjqcvOwvROerln5%2BVVtKSJuka36DWtsV9qfI10lJH2fcCsejfSMJyjaJ5s2bul1Ir1jihwhbaNf%2B4o5qN61I%2FvzF4%2BJLpNZd%2FEckHOKlaUK1TGbqN5XDHXUkvzPwia3spswQq1%2B764pCQDRH7quNS%2BXK1StnMjXmm8u7vukt6tJcA0g8vBlBnF7LyT9187O%2BC0rPSftbGc5LqnUJNCPJFHMtBbNigCNAu2ote4%2BZGs9AZVblv0ZBHvBS3vz6mivvX5cuQcGtRv9p2fFPqLZiVRPATYeRiu9P9pczA1jg7QEhK%2BgruuEROutO2A3Flg1p64k%2BhqJHHECWsO%2FPNvdfS2DmSol2CrRwzpgW2ZBKP4yNoENol9sMbKRMwTmmRO7cxw8rxDuQbIxaluOAuOElN87iT63lMDrwGmEpTo0WXg2On9jdIT0PTxlGtvzzZCGxSaQJfOvj2NOHY9qgwGIt3yhFASD%2BFW63GJ2fvGqQQCnGuWdQnf7NPn56VTYymtJrkGz1CyIwYwvukPoSPjp%2FRd07SgQdy28P6D7kA%2FQMchmR19Lu8PeVJhOThU0QaPLtk6srC%2FyjEBYFqBHgw5lQP2cM4tH%2BScHpqjxmkz%2BQ1k3ECQkJwxwry0bFoAL4uX6HALjvn%2FORDLLPnrEHyvwMni5fVEGHVUPn6IYtXPGFxYBP9BakXfNFLEirjrs3zwulReQV03aatHTDxHwxs0mLjN%2BOlWq24Hl6PF23ym0X%2BQx%2FMAUTVPliPhS2I%2BYi5cUNIZzUYOiwqVqs%2F9ln9lFQ7M%2F29FREX66KFQfk1kU0tS4RbsVT6tvt0PCNDW%2FFHn8eT6oD9TdmyDpGmkstXfgJIjGQQLHaaOVSoJSvaNIPtyTtuBpzm%2FFDIbBaQU6VDra1tcjoGprYqa06GLt2%2BHdtgBuBPEfa4hRwYLD8z4gg7kHkZP8UeUVy6tRCvcpJq7r0%2BnY4xeSnJBQxDtiEjpOeQ4tMW2NgCw36mPCsuOhUafCV%2BZSL1oa00BzfKkPNlujJVdJzFmMBhZfjHwZovwIliYU%2BAHOq8CcIccNxwxrEBXaBP2Sx%2FWCuo6RvufAoiUxh5xpAoIeTmrFd5wOTO05rzlY9HEIE6wrH%2Fsnfp9saGEmQPjut4ooeMhzyLY05HAHwCw1qFzTgFBsn%2F60b%2B7%2BGCJ07i%2BrVGM2xDQlUYI%2BTIkSIWnZ5Xj4UWRzoPoQZZfICo9KaimeTg75S%2FR5fzId3kpqhUm8fwXuvEE%2F90IVW5Jdab1D6HVT2ROkbe%2BtIUNwSZ1AWbV9ynIAX5Vd7%2BdGcu7JLY3QAestkuOhLJ9XwJPEXoiWu3uoN8Aqa3f8QR2OJrzD7zpQe5CSh1V5QY8Rmj%2B2Kugx9FDQnS%2BQCnEtqV0qPgQkrVehPTWJZINBpzZzOmbmSyRnZWvbj9%2FzQrDKZfqhUV9tZdjWsi6cPg49eeDb6%2BDWFXpkQOJzeakFvfooPBefhXsWVcWFhD6HBe%2FP%2B5zlx%2FSBCTLdSCTS3z4avydFun4S3dppGZPyAq5KfEj3XyclGwF%2BFydw8olBPu2IleNR1NEHuxkJmDq%2Bk0Yrx9WldfsbMrhjf%2BbVt%2Bu%2FafETLYHFJhGPUEudbYIFdD9werQqoO%2BDXTTuEw7fx3Ww5ZVFeezzDvIEYPKN4w321CFiqmuEtKuBR4SL4HXv8o81Q4YKyO5hsmBToRTWW0Y%2Bu9k1v%2FxvYV%2FfpN32ABZP4D%2FR9RFhMKxT1YJUmN%2BjgfKSz0BOAaL4%2F3wUqThgDMKHxc3mwZanLzU%2Bv86nfYKAtMsorKyzObYcVUrc%2B1kXbkmxME5fKRjA7vhjDDRar4%2BWzqe5cd3tM6HX4X2AR99WSR5Fa%2FTjYnfs3of%2BG4O9QbWHhE66hIF7Kk1iUr0usE13SEfQ93cf0yFm9LNHicQWM2EnBPL6VKMnH3mnznTIt%2BEsiZa0D3lAKn2sWuHmQC7HiO%2FAOUypzfkWJPuOnGJB9pT127cjPCL2Y5ODPLX722xpr70wKK7bxBEiuB0Fe4oAR005wkUIQHoEYFr%2FF1ejPbqxcP3Cnb5Ce%2B%2FiUouM%2BaY%2B%2Bw5iB445%2BQNvSMSPo5HxD49TIT1l4kl5J7OGxMG1R%2Blg4vI%2BTlnlO4%2F9%2FNCdFURzRyb6FNXX2zhwQuiziy1u0D7c6mtFk1y07XIF4d81V%2Fm96sxOZ%2Fs%2Bxjk7%2FBTGm5lfjQT76nNVim7%2BA9BgcDOJia4VC1%2F7eNaiR4WvHCYce5oAG2vvi%2Bf6Y05vX%2Bmq2U%2FDCL%2FzAXqd2Vu%2B9a02FxWH0mvioIk%2BZlFV%2B2HHQzLCF8LAoDqAeMR%2F9L%2BEMXqxOlYw26ddWvRt5OQuSHey%2FVpFtOZCwvPpmA8S8cgCQNQHzYzpui%2Bn7uSeH6b%2BE9D3O34bNjmDROe7tl3wpUh4QT3q8m1owlwZZ2zIZ41MUiPVYDR0nEIsGhMw%2F2baQpXU09rRx88OydZ0vV%2FcxNmOOsbzeqqgNGJT719GJd0b6ndudjVsMhkn0z0icSIV5FtiCp8HRaDpIdtvwBgdp%2Bsqjdd5hgItfnOIX5J2MFwYhBxydbN2B6S2SGllWzsLP7mkbRgdGHHMb6TrV4angVT8HhmaJmgD0IgSiCQC6%2BQCRRRMNeZpBGlHdCBxq7hJiXhXTda1AvOEOxKwhVHSIFMYjXfL9f%2FCCfQiqZN8iiUYVchvYZDbu3d3y8zzCWgWQ2Es4J%2Fglm%2FCwoF8Wbzb5LKXROoizxceFscfCqJaGGMFa0QyQKLMphqpFvw4sgzTpd9Az7wv0hTY%2FJ1RVs1MXd9v4pR6GsimS70EQ1J3dIS7kgrgaPww1L51cR7T1rGh%2FfOPgjJoh7b%2B7kpw978hUJ%2BxQ9jtlTaLVAa%2FZkGAn3G8exph73X31cWEpXqZRg%2B6z4ZjtFzdSqRQUOkVy4b8USO6cZlfC6IY9fD3l2TZbgdO9uf7A7y8shxyD%2FLSeU0aVlM1snWlRoOprk1B8T0GApvpSe5C0hbGrgiSCcGu6%2BU2lTmlFVmPJSfyD7gdwemhDC1vrjW%2Fuh9hLu8zcfNW1SBJnyEB6DaLLSZJz2LAwrlTyhHS%2BK76vhDJ5%2Fgep2IZXwqMmK0v8YTXtPXW5mTQV89SJFzZ0R%2BV4Lg%2BwITCrjozLcdWZtafeEzt2P1hSo7grhQ4YS4r4XSwULGokOAMmDWlcJGUcrxJx3RUjVTq6PZ4CR9J%2FEg2Xj7MCEJfih0G50ZlBmdMP7ddkrIZe4OfzjAnlwVlEbFtcwoASKphWHEyAU7lqpedOnN0oFE3C%2BpJPlh3q%2FFEYzYSQjhOnRe%2BBI52mHkNHMQH8hXDDY7k3xOE0z7Hk2519d7tTuAR84NeVjIeyYyWxbcZcpqb%2Bxvegbr2lXblB3dcXGqSP2Mdy7A9SudwAYQcmdaZAkr8gNGEr7WmeiwX0fEtVkf9H6mRC%2BymE8v80ulppC13RUfIfzeXhT485TMZJUSjbgAajzUkFiC4HTOCSxf1CLifY1zvtjhJQCfbgsbiQZAN22ViT3iQfX2988bgNgipVMkh4WzBwHqkKgeOVOO%2FEI3e%2BDd5IRFfUe0Yxihd26A1r7PRY8eD4TLlC6yRBCuMxknrKbRU549bDt9ZIeNIsDAzqSiep4RcOeBq7LiDsE1g1JBBOifCHDZweVMdLfBJp7MHYo52rk8ybQjm%2B9VSGA9kvTX783ybkTslw%2BMmnzwwUaGEt0Ez8uq7iKpfn777FiAdReX3B%2FaFDYcwxEZaX9TOhhefkVmVKD5OpCd6QJFvBtp4n2t0Ow9EarZumZPhnuwm%2FOFN7GokHk%2BbOSj1ipovpSS5RGuF0%2FQ1Bs2tnFXndnIgieyvxIv05cZT6a%2BTEU5C0MUOE%2BGR2z7s%2FTeYUOu94%2F5bl6rZXMM4l95HjEST9Uz%2BQ6ynlQ6eHz9UrLamBehznRZejOWu%2FWg6ypqQwoh82s9Rxyoetebh%2Fk7ybQcwY4gRjsD850qSgfoJHn8amiuTQGhdvhr1HvHH5uFXip8NHnBV83KuIuBlPBAN9YDnJhDfBAJgWRN1yz9vpoKkp2312j8EDMtzVabYSdvkyf4KtSxA4ULKyjYpi%2B16SaclHkT3sLi7G1GhmkrSYfwT2K8hzJohoHKG5PwQauGDIGYGkNWmMuyZhXAdIrJL4EJMVpbiwGJBxmfUvm5a1soN%2BULwpA1%2BrvVuVBN45ksq2PlNcx%2FGY782QK%2BL7y5ZCPex9ybDnqW9VFC1VaO94mfLBwv3fvGSObJY%2FNIg4ISPlSBMM23hx4jXcaSleTDxHDN5Ktd90Pc3QrQIecLtiNlYFLz%2FC%2BPMbNAK6M8heYqSB%2FjCr8piePIngsHX4MAE%2Ft8MzUGFRBTkgcaGvhF6wUPK7732iOxPFUAXAe1hhv0UiO5w6HaC4T0LGR0GsruAJRU%2BtYFFMhCquTIJDpqV2L8ljzXD0RCz8JPwzYBEPOEt6Rvyba1bn3AADO5AttMmYjDizzbKdH2CoVW%2BE4MefRR2dRpH9BhzDGHIv14EW9wjFfiqVIQ3Tf79AfwZWMGh8WYyb8tVACP8zegSMaGWAvibAZ0CFP4Z60xoJDHZEBVbJEttRk9BQJCBhQAkQ3M513BT2mPIsm1%2FTJ1fA%2Fc5wvo7rH07AgpmZnRBx6u3c1n0nqki9sTSg1xckB5OxF6yqAosLvvwRyUqJyynMNPLBcnZ5XARNcQ%2FEHtAf3QFsdALhtG%2FZ8%2BqR8aIdvZ6A5ig3H8DcB8aPug9w4GT2ueIA3qpgWOkdilr3ZZfGE%2F0MrlOx73ZsRxFB1%2FF8VnhGNW4DWi4eJjtFyUAAB470eIzmfvBoESjBhUhmnjt1Cw%2BEBs2ZDW0SRfOXPFduMbRIVUYhWv3HzA%2FttT3fgndQ%2Bt3sqPW6Azwa0lUBMxCW9UxHAcLsnIyyerenc6Ujkjp3iuH5nGsFpUcfTIWIrnj67ZSin%2BqU%2F6iylDANlcMc%2B2dLIGb3QdpLNM4rVttTimemjWXsTirZc91CNrFvYNFVw4MLs2hFA0Zmmql8tuQQKNQMiWKiDzkBfvvthFKly2zMgxJStd4wTgK9hX%2FZtvE5YnZC%2FNyW76GJQsStmdvfguFE0BGi9xCSKzypNyYcJzUcLOfbexX%2Blf41g7%2BvEWJm5rMdocOOlij%2BxytxB0XJIIkjzlOK5yJd3HFas2bKlbeo%2FngsObljmlLdBHJA4dmey4sxaRmH2xktUC0DZxGJnUTB%2F4sa5JfFPV0fheWu0ZnhFSHiNkLsCN4trkI%2FIfCucXHP%2Bf4P7D3A8rJw9fgDfzHLyhi7uBbkh%2Fs8Ou4H05nhNQcDnLIOxPK7nMqaynEFsIyyGR9siVOKxuwtiuLxGd8852iuO9w%2FG4wrWIKdlxPcp2bH4INcq%2ByePG%2Faahs%2B7kHBQjqjvn01%2BFCn2nIEhVEK%2ByUPtOmLiR03RFYT75v3BUOOGmEuot19tq9hQ%2Bn%2BvBUqJf%2BHgUZ9IsBrt83JrVsBnpOH3XwuPe42LfB2oF57sIifl1QEnn3feZ%2FdiLmAyLhiX1WjukOhftUfC0%2F6ufSs4VPusdfCj9UvwB5ohOZZbQkpNqLT4dgfGo9stza8JedcGl9C7eCpexkXYld6V3mcaI3ZfGFRYKp0xUpTJvXefB17nNsF1JP0hzJXymdXtU3sQM9qinsUiXdSilw8X0hU%2BKbf6wn4bmVs0tQQiBRc3Vt0kUzJMQ%2FDvVJtfCwI0Wdl5DDr60iBYTG%2B3fZ1CDWnL%2FKUP8hg9Gmn98S6cVwa18FqX06EbMcV%2F1hDQtWzFBjs8w%2FJPiRos%2BtSanXBRfZLFZnRqZHj6pExLuBfQFpBJMfE6sJ%2Bg%2FB1pBN8jhnvo1JjqwfXMII2W%2FAum3NQgV9P06qnv11Fp1s0C%2F79Il8X%2BikZtJYm3v9YjIQ5FeLG37Q%2BNbgp0NBHDGhb0l%2FaO1dxEAmHktd2zzXe3%2FrOGYZXfRKnzS6YHfiIdbAGi13UdvRKuVRgr526ITqffrio%2BLUkwh1KyCykPyq1UZstRuk7o7dR9%2FARhoQOBXp1lgHJikuSWZeroVCxwG9bh9ke1cAp0dk1tI43048xX4DDGZd9S6l82dlfsaECYCfXz7Q96GMRNSskm3623TGS6KWGiRIUYhhNoXy6nci0z33GnYNQBILtnrSDhzkNfPDehoyyqvdOZOWCCCQHXEvEGTe6JCTnkfTESCoxmHiZlY7XeMMnwXr21jGC7EQhfbJPS7egq3xbAHwt%2BIduk22rrall%2F0%2Bmya9NXcYT%2BgA2q1o17rLXb1i3x8Wu%2F7Bc8OAXXhObBVfOjxj48x8Sn2wTdIbdmNiNdx0PpOy7f93ItttjBDlpeSzy7mnlZuQlS5b%2F1Eb4YjP%2BBeVhijpEzWHuy7Ad2sV7%2BofMDL6nwVdx3shjujSO1%2FQwrzUi%2FJ%2FzHFebmlcpH5wXURn347G9MeE2C4da5ttjpjkjXf9tI3Fo9FmnH1pzN9E74BVkbnosKtx0nL9Y4mn0v6%2FVFsWHfZtWWT3pX9Hz4QoO3Xp3AYtjk%2FMyJZdx6AL6xt2Ic%2BF1V73HbrFVyZCiFYTww2Y%2BEIGkOu983EEuu6phUTrpPt%2BJ%2BNDQQd3BkcH15p9pG2fZXlfjmN672oTuP%2BoinoMe4pJ1Ujpsiu2gLE9PwQw6c4leW%2BBW9S%2BkU%2FSaWXlZEmUiBbLLVl9AfEmtShB06eiL93kzSAkDuG9zNJMrmdHOPcdFtFPFzF0n4bhPvrvqyvx%2BZrwpd0v%2FfXMRgsuUl%2FfLqdQphsMpr0mUg%2FpAneftecuSqkhlkYU%2FCXazlUV%2BGyvlfIC1g8DeQcA71Yi7M2LkguCIGzh2bJV%2B3NkLwM0XbmT6Wc0IRusY%2BrbP3ugu1kJe%2B9n9UIba1KjVo%2Flz1bHGncODVM19KsUOZCTujqbVbvTiN9uDg%2FGTlM7rWNfZt7YxED7s8hThzjRvBG2LTmUN%2FyTO5FNj5CQDbBt15Rhu6S93URMZykunue0pOqr3S1M8ihdvx2xJk1Czrjqr6Fq6mICA6euTsnDTsue%2BQkRK1s1bjbNTDMdgRLIzB3Ue1hIvIjAtHzqi6q2DuLH5ZXCsTKsw9bKX92UAv0Q3wPgEA305dmq8RLqNwFEIWT3L9cCZDdfcYgJSALViTP7Seg87AwQJw8VKboJOdg%2BzM5fMdvFr6C9MZkyGt4bU6g%2BNR5jnXzvmAg3cY1EQA1cmyNQUb7cVlukUkmfPX99Ka%2BcBM14CM6yFY6YZsoM%2FMkcCoho0E2CsIjI6gvOToiGQet8Ak%2Fm43qK9GVnZWvXy1u6LhRGnUwp2wwjCrXiNGoKvnMYIt3kX9xQaHnd2fgL%2FKHx3H9WtXCIROpQL5YEeTFisXFkA730XHW4SLoIIjT%2B%2Br2iHPd8DD%2Fv4TUdmxZP2u%2B%2FpcgDZQ2QXze87kQRXb67nchxT33JdUn0BtDtyG4TfjFRzm3pCJYrF1LLAWbnCUEzygGx2yIM%2F%2B7CgLrfMO8dxFe3FvxX2UwPW3JfvE2gqPkD5CaJnB1DEyUvTfIzhPnKHmYaTC5Cx9Zog0g7uhQUHiOjJijDfPYc8BTwoaB7wCbir9QDbu1TjrX4Iz76t0t5IkYXm44Oh8Ht83phU3Q%2BxODFvJoOJVac30ZtZ9TAdt9s8Z%2Fial3lDqCa0Ma8f1y37tLWqth2x9cgP112TXb8Fzl9oYdj3%2BdCwBOeqg10cpJbCpksy804QMYLE136jP%2FJ6ZE5Y4j2T0yO2CDTM%2FT1eCz8cytQynaPIMQ0kDGoQSHsF8%2BMfEeO36bqKOQ%2F%2FkTGPvnjSxLCEugdDGw3gDOY5BcBDOGezOsTaPjv5pRWt7MoQXSwwhnRjt%2F7mfT65etHE8BA0Oo0YNIr87cpY1R9%2BhHK8TaSQx%2FZj5CCpQaQlSbUEC%2BXhF8WzWGLWAZY0dG9Q9DQBWPHcJHYlhbY%2FjpbZpKaAVVdkYcrkwJbVYqeXsz8VGZzfyoQHBIlVO5myfALupL9UJUqGrBElDxiXFVBIne9R3ztB8tgM9Ln9sbGIqJpiXkSZQG17eKMQVcQNgj7PkpTBzwI%2BLSgPDidGZ4mXxkhKEaRixh%2BfCtEhkvvshlrGJDwCDndTm7AfV6R0lnPO08zDqzmaKol1MWfdTbfUty1BVuJVDEZpbOeUlG6qpXT9tTTqOgJkdfQZSd90IcfZRn09uL9fUxLCn8xGh4%2F1KIcU%2Fb71yY1tLAy28qXQ8sx8ADiOg2posAqyMbs6pdiyJkvQO2N57RkVqzI5u%2FlW1%2BEgRb0KlJukSrpgvAXJCLkj9WxTRHfOm%2BzKcwaLFZgMZ7whIqwHaWxY0k9YnzfkTJy4qFO5nlMUoPoYYfOpuDu%2BnsFSM0ZBBHv82y2wb6GAOQ%2Bf8phjl33m1CJ751lDttR7sYY8Z5hzRVXyuJLqINe98yaUXF1HR5%2BbMd0%2Bv%2FfcUS9yvrcPyIu5jiotMQyq6KW6Vxjg%2FGwiYb40qbZPeFn6VFl4dxzc5yS99nlVNrosZi0rss9GdBzIxCSqwRacqEXgKOEcw0opkTYh1%2FYrN7O7zJ%2FkvT9Kr6%2FYsRTqjaRn5CdO8VdCK9MgxlyjzrH7M%2FkvUP6vTYnU4SCkr4wAZrNCS3QySpOZdienNAFas5ozueSTvzYx9hD6duwLDvTb8Y8X9AnitjkKY6Z1kKvMlt0AxTFVJGKdt5TApMX0B%2Bo71ISNalXRbt2lrvvsAQZmtNfWiRkMCgm7mUKa8c5%2FGHQTphyAorrFpIMX2jkN%2BtP64MTAKvLnYWP9LRbXBU2NeZ3qrjn%2BcT4O94fpPnk%2Bjo5OqE5jluzAfHYKZKyUHg6UWf9vtLDOHQeEQj0a1xSj2QJ4WkOvspeO1U3tzCx3C2rtZwDHd2IKEX3Be3kEG3PyXQqmTOmDN87hgMS9ufKDmlNdqN22plj%2F1dsQs4DXZ%2BO2esjR8lgeX2YdwQEMVQsVMqoWtqBuM0BOJm2UmYJ5Wypn5QiqevuXEFFoZK3Dv0xyGGmIbIMw8zi3KadV5xPGsVZ6AG32xw%2FQRdUViN%2FeGVT%2FDezPODyDbZT1LJC5gLvhkkPhWhvMcA%2Fs%2BB5mv%2Bydp%2BkoSqY6HcMF%2FE9b4Kf2VDG%2BOGeWswA23%2FYv7lYMQcZ%2FX3Xl1Sw%2FZsRVkzaBuh%2FJFHzioigKIAF28%2B3I4v%2FJOs0KRRWneJyQNOi%2B589B%2BPlp1OPsKVyaKRFJOHgohbldcYVA6QJdNrouZrvnAUBNfkZgEddqZab7SUS%2BuqBsoXPCK5tO0T39j%2BkCFRbmgLTxZABVBfrRpQhzHz4X23KrpsXocmGAEk639EeHHgnRxo0dgmTqMF%2B45BMcudmPxeXoljbV1ZN3ss6KhYezuwdEiCt%2B7VdnaokI%2BucS4jT4ZKRE7QOaSYX7Ey7aHZ653GtCxs%2FO7N2jS0d8Q4IrZPBNEMrMQTIg9oyx2Oy5%2FfNTe%2BSepdnLu8fqOTYM4nYVImGyVeDMXm5gy5fp9RGnbuMGw7x5%2BY7pC0Yg7njYtgyMHPJwGVji3osv0kQZXoBc5YafjdeCdGtJ8%2FqK9yvpAlLLPKurwGBNdrTh27eis5mV7RvfhlZzthaM%2BJW3O8ENytHmlDOHR95bWih%2FtPBXWVfZ3TRY4XCn%2Bi92yFboe0zCzf90uVwo3FRTrCB35xZoYQI8YEaK6Zeyji1snAGX%2F1J5o6gqocjcJI3egeLwfB27MdiQiAWzW2g42mPjdNFtmn2UF6UeiOszyyPgYuhcz9FYV%2FukR%2B50FSMwE3Okl39%2BQ7j7JTWhskoaVWulXm%2BNdHZOZ3CB27q%2B6qMwOcmSmaAnGG%2FLt8EhHkFB9jl7l0owXAIKU9qYPobRy8Wc9PmVBptN5mKswJpwgC2X3onvE67hS%2BXI1MX06T0bMdIUa4G5PJN6r%2BESeLCDFjNlUuBYLKz44yNPzKlS4jdAtnsw8WQma3rgDXHx%2FkkZJJDoKj9%2FH3yRXrS4l8ajWYg%2BCGLWbKOoHplVYZ2AG2UD%2FkVmnbtIx8k46yQbwxdD%2F0T3H0sS1TfuBh5JybOi1sCIxTREEEv1P%2FekxS8TTm6Pk07zlPv2aVh9rJZkiasjbe2Bf%2BAtTzSQqLeUhPYpWbFHGfOQ4rkYiARDru85em25EA61K2rT5zhx3nto4I1HGlu2%2B3rPOsCS3%2BPTwS43EHbVifw017DAe67eDXJTBWaqs%2BoSOMBEBDY3BI1KGmTvp2TqZrCUy%2BOplVJGefOOIls%2FHb2%2BXhk5PHimEcFo5Cg3DEfY3RRX8BrUtZfSnOTHzxV0GN1W5IlfdZxE72CC6C8FNX4cz5Vau7Asp2yeJB3%2BRKGPpsTHgVaQ6UnV5Mr0YflZQ65%2FHRKEVsF%2FDEfpI9CFKQyB0RgcXfqjEcyfsi6E4BLS7D06%2FaHtoYYG2exqGze%2Ff%2Blk%2BroThwDTQqAzFcNh8qX3%2FJcgsLWfiRKqTVG4Bb7FsZ%2B%2BB5fzZsnxtw9Pyu5T7dUU83Rnh3uF%2B%2BS8GtTLwYzI0Nvzyi3z%2BSuH25LsAGQbzxBZe3CqE5AdDERKF14bShQP6pITHuQE4mLmMFCu2rdpD53rZNV7MfXQ7POCR10yxdkRhrlm3XhZATvIOmHxuTlfagCCyB0b4QkeX5GObvgAzUM%2BBQj8hgDygiG14FaCvGTmg2XyqI6kbDgNGLFiMgYY4TZZIF1JCfKAhiree0PzSn6f%2BaX9Ro84JRoy7sjH4aazWpIdMfrJtjXPk5td0AxsgvpODxu63WQ0cX6LGdacjNPA636zvtIb%2F%2F0Rb05gOxWeBqCc5RxC9GPoIz0wWRW021ipjNA%2FWz6zTurAJJz6Su6YtdjjiRwLjXbxCeYvIvAeQFiA4JoAZo4ox3H2dtr5Ejxl1N58cDkoXx7K%2FDTl7nSukjB2Bs9L01pCmTvr4IHs8wedLnN%2FYdp0v%2B%2B4lzGxMUZ2E573r11udA1XxbklH41kvCt%2F%2BEfwmEViKQduuhfQ4gqC2MT%2FFwhcpvgvJNCGteH7wxIKp%2BO2MNrrCeph3MvPGxKGeBjmfwiHCDxscBpnST%2B1uNyPv3F8oso3d%2FUhsN2giCZNlOzWCGgHkhuz9zn0zifyS8qv2cxIufXu8MqAZJ5X6NSuqMaGbk1aJloCpKWjyX6QzqXn0KF621ND%2B8%2FXKM3dmLLJEvg54dfjFcrzE9AhBQZsIBfhZFD%2FBhnLXxB8NrqA6voiz0FWh5F6%2FHIzIqKh6zbWabdVNlqSyA2nmBm3zPvjRIHl2SreCgjjwhGtbGqApvOk3x7X5w6dy19vjRrJy3NsIqATQ66BxButVs00tqp9RxeYUeVUClBumiNAxcv5Ilt%2BITjqUES0F1kP8bKxksQUNiksK3OVLqmYY5m1V%2BxUeqTXoFF9BukKk35q%2BSDRYWkDzMcoOA%2B3Tv9i0shYNsnuYLbaFgy8Dk%2BiHO2q2iX3iXGkpnh08PxJgFAdW8eimBNdcCjQT9hymEPXReodey%2F1iSlLk2PdZJBFrDk3XlegzJte%2FDJ%2B0zqlecHJBO1xD3XqAAC0PGg0i6f2MqpRPF0XTEUTEzNOaMK4raL9F0FZY0JVn7XIASHOyXxVfYeEXIkQfHFOaz4fGCTz2%2BAvjmqe%2FpLnNka%2FrQbHhVbyhNjBESx3aTxwNPahKKidd8TRyAAxWFE9eux%2BRh4zKJJW4pZjCMDRA0pB53lqdtGpHI2y7lNfIdiSvtoLzgZHJAXHyxBf%2FK5GjpbDh4mXvRyBnyLcDU3kHS%2FpTPNr%2FztvZwhoXw7ZIZWNalFoo6eUc7Hot3hlDjnyTDpPYIaFa50ihRGaoGxO1QGftw%2BFu7Gfee%2BGGC5qjXNxaC4Nq6ULdrVABiFKisjCiHDXTkkxlwYby3hlFgw2xwcUHM4gJ%2BJYGYLoZOeccnAsSeLL6IaJNsxeoUmCrRmuPESIwfrQVvqayZrmaGzU0GXT0Apka%2FwIRgipF1D7%2F6T%2BV9Ih9eu1MZKtzmLHDGe3SMBs4eDx1Wyl4qTK4C9KSWKp537BcFsT4yUYMQETn1K14M7tYDslOe0e8%2FicCoPh%2Bt9Baj38O8qixL7pTQHB748ktOPJPM9OKxxt2EeKvHshxCjzlgXQE8Cp2Fquj0iMGE7cpNd8sw52csACR0l06R2g4z2BthL8D77ev%2FhkdndkJR6%2Bn%2FnpMN5XJr6v%2FgAec%2FkihOjKepunTqJjaw%2B0xcjAU70EPkb4ZlTp%2BEH7irq39hKls0fmJ%2BOZ%2BgwaKtNDZrc%2BAc0AOjGkUQP9hEIFjxInPfOuA4ptBg%2FcL7nkfQyUId5CehgDuiILTq2k9pnPU0WvdZ7KBRvX%2BxfKq5z2V%2BgobmK%2F4%2BLn3dQvatWkJ60CrXAUKw7yrKACBx5Ww6psIEAc4344r35u2mMGZ1vZFREd8twmr1cmVuzZAoGEYjFYZloA35CJinp8S5plGQyDJU4PdN8yjVIH6AYzTmumqWCbkFDpIO2MBRapEZRkEfUrOmVvn1BTMdQ6YhaeIUGwrJghIdtZ%2FIgo4h01rndj9MHa7IamMM8el27gVtqVfE2791jVMieH0bo6k3syi2rMnAsg%2F%2FJaAM3oW816a%2F2iUVtGBbBACZnD993Ib6X1EJz7YHeA%2FGvtvPPZnbQ%2F5KkAx6%2B6J3FSudH4cWg7zvL6W2ph6vlOlYAQDGi%2FK6VzBOEgzTJDXw%2BoBi0jnKtkOQpsOtP7yp6xZF%2BoYJ0P%2BaHwYM3sbxhL4e9njGRHuqwWQVqUdd8Ecge1yiVjXMeBg1kDYOyiD9LxSwlM4cVR%2FECqF0iScSVoqwQaxRMloVSSakHjP%2FVgtjUB%2FtmU7BhYwrLIpFEmGoPVm4tW%2B4azwbhQUK%2FRtYO6SiWKH59DyqrgBR2INJh6uJQzpgeWEh%2BIrkIRJcq3YhKxIO0HkXZ6mepWCz00sWhUex0L81X9YwQXNVE6u5pTZNspsd4ODeZDvv%2BeSMeWBFkmS%2B%2B1b7ZuU6u%2B%2FC1zC61UA0J7iMGq9BRjmr%2FN8jrUanzI%2B9SfA7Uqg5nH3hGE2yW7vvuVlWr4rEdxVfLksJzauvHGakbX7zn1FOVQ1KI4Cb5T664YPm%2FGB9ptKH1OWEKR%2BG4m7s%2FkI8%2BUKa7TW71irUP7RGK55rIRR%2F9f6b2R19qkDc10AdIpv8n8nwqyxtt9zr%2Fvlga8SOuwvrKGkBoVUDKnhpTjhMZhM7kwVFp6tmbIZZ141iuUSgPDOedVWMrIRGAqmZ7g4dvnPOm1rZRmHpg6XNWq9ryfoZFCoNwMwZHQ5bApzuzOoFjSfPCGmOFYdRAXNRPjV49%2B2aT4RKhAD0biIMVODJGHHGSnW0%2Blch2JX%2Bvsiqmk091a9eop339hDvtZgn1VYTQyzI7FFyyZvKrwbdr0fdSJ1DC1hDvaQ2wrFCnfia4Fr5FpyRXGuCIv%2F1Xu5LDN%2BaHioBJd4e4db8ufx%2BT4XdalsbAnwU0E7DgtqWliHFuZ2BYVmy7r%2FT%2BnfQf3jG%2BvDxmd5wHpBgOs9%2Bk30A%2B6RNSJ0PYZ4yaGd4Jnj8681wKmc0nWNbfCq2%2BooT2LPzoimXhE93Kd02Dl8NCCKiIfTmdoMO8lVrOn5vYxfSYZZ%2BiykrIgrxVnlVqiy3KKc2c6XOri%2BV08Lrnc6QiU6oHE2Ugzi15Y%2BKk5wBRX6u8%2FOv%2F%2FzzZxNzGSknU8BuXYkCRb%2FEFBrQ1VXASlIaWDBjZzZtF5zoZjhwQc6vnNnV5DX6cjvN2tcVTzZyFFuN1qA6UK2JdeMBMSegkDQdaYr%2BPA%2FXJVnMqcF7m%2Fb%2BruMDzqZ04UkhN13XeBdUrC84i5CIH3FghrDgm7v5xAkmGDwOo%2FFytATqMmbp1xJD6kzCxVnP3dxtRFQbjOkNEo9Q5iJnEk0tQfaY%2FaLxrhiEsbEL7um%2FOo9AtqrCu6Rq5r6ExClgGjh6Bn%2Bwg5sW%2BEMML%2Bj%2BKpQ%3D&__VIEWSTATEGENERATOR=F5605978&__SCROLLPOSITIONX=0&__SCROLLPOSITIONY=0&__EVENTVALIDATION=KOJAfF2Q1knhWFz5MvfWclt3RwVdaMWigeLSfkLv3OtEUALrryE6O55FwnW0xeg7aq2dSyB1MoJC%2BNOzvvgHMJ8kuzPZSYWUdyXws5lwsepWF2hmprbCRZXCK29n2G%2FerpZfn8%2FDf%2BjDUDqTjp3EBrzpCVIV%2BHocJxP365Aax9XUk27taFb9KoU%2BVqZhZQvohydhPo%2BGHG0Qq%2FizVjIOPzmO4H4DBLG6gNsSrH1xUbWyY4A%2BQZMubrbp9xjTmCz2KadkpweTCoqyfK7%2F8jF2Foxw9ZA21LwyeKZNLViORzMkXz9rnoF6%2BInfdk2Z5TlacDsNvy3a1YGqDVts6JoVNa7IJu6S2aqUcQIxibi4AZuiASDNG9xbn0zsSEJlqMqkGeBG6h9OS1hvGqPIvKdCCMHZl6%2BT8B06ZUuTJc5Uw5d1hmSrgUzwu7Gnvr%2BCMHsWjH8bePTsqgjByinlobf7AkO3yzesee8D03Cl4qLiVf6frj84dWG3Pcrlw54%2BHzge592nyFSzxQf4d7dLZIx8%2BPGGOusf%2BekCq2IY7fT8seojg6Ivog8hHYO3%2BcY3xozwW%2Fcadig%2BqemmfkHJwWfgB0Wk51iII0FiAMe1CkSxFJ8J6%2Fefcn4yf1bXJlTfsKZeQmtco8u0ogoxK7FsMustlHQBJdns1v7lYmuhYL96VvZS6OS7EEapfwEl2adXdm3IyGqWM8F%2BQThhrDuEn6Kthpph8NaW0UquTJUHQAvMVtaHBbDRv4AiL1Y0C%2F9eONqnI%2FaPjE%2FpyTRTG635gakKuXRtDz6XeV2f5B7KfEhdPcP5byrVOFg31eoPNdsW30elNi0Fz6PKpCO8E1TW9tG95EQZHZROtQHL%2FAiX0ARviYVzq7P0p8YKEke4RwYA8DQfp%2BPtResgDXkj8NIuaHHzG1SUza5OKZHBwcpMcoz60FxzuvCeNDxTmXqt7LZOr9y8KGDA4wYwkEC%2BUQd1iKNaLipP7p39Ov%2BHy9bg12n9q9Bq%2B1muGWKXVfYXpYqrbxLTgUInyDeOPQ11BS6rNovNgbsJ66mcpoFLgaNRc%2B1XTxsrIzfNatPsf9jo%2F0bPKmEcXglv7Kb1ksifPJeFi%2B59Q1V6Z%2Bn2JUHCu5Uf5q2kiWjy2ofXL9170gD%2F9BNh1pPUjO4mV66dm%2BVhohlSVaIRTWKOv9q3OWRiSVthcdDnIGO0wo0FzHSo4j10n4XblpPYd1qP2b9iRux3T4mqm9XCxhYUc%2B1qMPbcy8goqncqtB8eP5glx4gu4ho6hUuIf29hKtMchS6V4JRDLA98nB5pYd22UdBUEWci1wJgP0DfxQMlDFHtWApN6uGDFMvSZAqF9vm7a5CvWAZv6TZcWVCJft9yYa0vPpWx0PhNAeAeOerqY4DP%2FV%2BvKZ7i6cfzJ38OOxApJmJRKZNmoUgpbve3QBMbQje2Z5wobltTNyVCHehIapiBfOn0hI5aghHDX3YpkTCYDShqSx7FCfs4C1orPJuxfaJ%2BArC2LKB%2BMSDTCK8AS3dUtOsqheo9qqoz10BXgJ7aiArPvXieIQ%2BogywoH0MNuzDkPqHUOrowd35ePSMYw5aOChAisKyBnseBbk2%2BbAtkNwz4piUyRdX%2FBLdvvd6%2B%2FNzJGNpNDWb3baBA%2B7wx2hlA15X%2B55%2FAumxZqWf8bgjNioLgeT%2F7tDPg161%2BNS%2BOCU18WvH9xFHdCrMPLf9Og0pUzOlAFHqM30N1A6SKBpOB%2BeWCmQU%2BUuhTlA8Xl7jr6beW9Ln89FDkz%2BpA%2BZoC86gP4Xti2iRr9cYWyFNB3KqD4IvtugkdQ8x3dvznXMiO1gLvTNT8naa5%2BeL%2B0WNmvqcy4adUisDzi3GynAQRGn21nZD7CMIR6iWiDMTw4DK8WrhZEEJckXUGEQI7QmQ0JfD%2FqOmsDAKb%2FEigXbQrCv8zS0c0G4bPuO5M5sfUtk8xLjGgG%2Fb16bGqMUmSTUhdDq92Ue6LNuZX9%2B9abBMt3tpVjnS%2BiK1a7qwvD4HuCa%2FACeRtVoD0635UJPSZCU%2FvE7t4hHTHDXMkXNRV5izPRWfBWTNV85aTgcethOjXwP7U3nAPyGtE0HMMiJvR74kKG%2BNIFciWcUezZSj3ktzv7popEWe4h1zBGGa4E2jR954S1tJRkkM6Rpzw6H0%2FS1rno%2Btlg3BksbfTc2EV9y%2Bw37iaafM864o%2BxNOLr41u1GXsp7GY8ZsyrWo1mboTBgE%2FqvuTtjWQYX6l%2FaXV4BGAVct3ZCIrDPAKDj5O2MJW4km3BUOq3RTvSRqDh4Lrx37K%2B2KvTITDuBKNkVPKZx%2Fi8XlxdxL1w2MFCAKXsV%2FgrqAcdhciURtvVi%2F%2BHzWUWG0Fof3ER9p%2FX14W27RUHMrT7wFoP2k9dC9LzY%2FU%2BL48D6kHO0K62vHQAgY3dtefxmtyr%2FQ38qEw53aTqiiWqVMhT9tZOKCWUHONrvKPYZhrwB9WK6gzwG4DNBS0i3bFo69P3Q3sWmlLpECEJscT0h%2F188lKHAxw5%2Fy4bxS3iYM6P4Wbe15jySP%2BGi1XGKQtHIHUpes%2BXhWpfgYotCClr9Pkx8t0gXpDJCjltnFSs9Jd97pu5bSme0WLF1%2BBJsvfB4Bocs%2Fp%2B8ohkOGOmpP9J%2BaCz2BGc2mKoOFfdKDkSHTMqdrP3asboxj3jJN2jh2cXcltnTmOXZh4j%2FkrQbK4zlwklfPPjpOFQixE2VP%2BHxEN6ZpSySqFGq9ivrY4RV7HpuMoB18ed1MxyC7WNnqIeQBFxRHx2Nq3ktB8qRcRtn2cqH9p2Fq7l0U0FBCGbyaPKVn8HXND3bsoLeAEl5HFzZ0Im1GcfAqX3smduYrSSzwBMicMu4BMbnyzlGN1fqdRj%2BvD3RSpNAKqMyRpOiBDvDVEfCql56VvnZwnI3PeRxHgFkkiY6i1I4Y0S0K%2FiY4NJmXBoGtOIi2hYHT1hXY8oFm%2BAY4TKppAu9t1EafiN4f64DI78ZOh2OC8SnMsrf1uRzOHtJCZ6Rq%2BjjKnssQrIuoSmw%2BhOVzVuzopA1I8maRZzyh8otj7IzrKLQncUfZB6e9HNZ5yX%2FB9Tgowb2ki9uktOYk1FutSnL9nRr0enCB6xRZHrkYeKqXx3ciazHUIpHqRIp%2BXeBP94VbB2CMrFKW4D1wKXWvJY%2BQO9xSjkB%2B1G1N%2BGO8KGUz4AtrK1TGo76AE00lGneYmN00icmMI5weB%2F2opTsuf7QXGKZXe0bUdTujh%2BX8iP9ya6h%2BDXs7ELAZerEvvMujXmbcbxjzdKEsb%2FhYgTDA6DWsVkiKxl0kfL75DdaKxhG819zhmqXuQworYZs91l29T%2FFOSF5dotuQXBsFVHbbLBThKNrN3%2FoHogoQTPkUzYhWHJQ0il1ELx7wHdsysFWTQoU6vAGF3wxQKjhvIUpyBT7dvUzmthusKFedIYtuRTYYxm4hZbprJnNGKfQA%2Fxe3ISqRz%2FSHZy%2BPJEgNhg8bTepcP2WrWblLSCEYzWX3W9nDi59OMwvYRSjoiOxN5aBpKwweRMSU8g%2FTa9koNPbuzIQZ7PekWN6dQQoBipMfMhhpWjdiSrjMy0c1xRZguYGy0abDuObtznzbZWpXThYC3%2FdRfCawa%2BcRr85UzYlccxMjdIh%2FwfFFAwXTnF1X33dYhj6Te3p%2FbC3nQNvKStq%2FRE%2FxFDlu85zQbwLiAlKsFRZeCjkd0%2BwadRQGgmAPme8MeBIKAMYEA1Axrt5ZRtu7PTrZeYpnb5HamfcS7dOfP%2FjusC%2BP7NpObyLOnSkzz1%2BsZaurROKxmgDmkvxf%2BkJPyuJGBG1mHrbp1AgDrjfddEfRugeiAYGMY8LahhCQYOmHZfBM%2B1nP67Eyhy6yrSdZWkdYHZrQnNyt6GNL4JfJx%2FNShboDzONHLOW%2F9pknGatXDK7oo%2BQCgN7zVmoRL9TnyBsq9z3P8lXPnPnN3R%2BWVio8NywceEvo7dRIwFbc5MBk%2FjdtAiN2WNsRGfTmbWArHkYsenz42xUi2KDU7ToBwYwVebGSmOvL105Rlfl4PsXvnjRkqYLKto700jZntm3g3828ojcSMFuuFwYO91Euf27fOWrsTzVJiMwwxbqaEV8ok5wkYhhXQHz5oAcBMK98FCHonx2jlQoFZfB7gXi%2B7WwKItX0XSLbWWzplJmhM7C1fGZyyD7EfUAibDich2NdfraHy7lMiW%2BcITo4C0A21X9jjmUBO7W4eDZ2bwtM%2BaQ3KvkHCaexpjBfX7VKzSeHANv7mNa8tQ3wkdpoElJHPEkC%2BsoIAJqfZTpULIM2qpzOnZXVssD9Qzrcsm8pb9ursVwyWg5GRMdcQyYO8m%2Ft4r7qGz%2FGspYm0vZpvhYxTwb%2B7EJN3%2BqS0coA1jleXemke1LoCzRSNRbz0d3ZKgy3MHCLc4gh6JvpSmcQ%3D&ctl00%24MainContent%24courseTitleKeywordsInput=&ctl00%24MainContent%24semesterDDL=202620&ctl00%24MainContent%24SubjectCbl%240=All%20Subjects&ctl00%24MainContent%24crnInput=&__ASYNCPOST=true&"
    course_headers = {
    'accept': '*/*',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'cache-control': 'no-cache',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'origin': 'https://webapps.wichita.edu',
    'referer': 'https://webapps.wichita.edu/CourseSearch/CourseSearch',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    'x-microsoftajax': 'Delta=true',
    'x-requested-with': 'XMLHttpRequest',
    }

    directory_url = "https://wsutech.edu/contact/employee-directory/"

    calendar_url ="https://wsutech.edu/admissions/academic-calendar/?gad_campaignid=21858591768"

    def start_requests(self):

        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        if mode == 'course':
            yield scrapy.Request(url=self.course_url,method="POST",headers=self.course_headers,body=json.dumps(self.course_payload),callback=self.parse_course)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)
        
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url,method="POST",headers=self.course_headers,body=json.dumps(self.course_payload),callback=self.parse_course)
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url,method="POST",headers=self.course_headers,body=json.dumps(self.course_payload),callback=self.parse_course)
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory)
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)

        else:
            yield scrapy.Request(url=self.course_url,method="POST",headers=self.course_headers,body=json.dumps(self.course_payload),callback=self.parse_course)
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory)
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)

       
    def parse_course(self,response):

        """
        Parse course data using Scrapy response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Course Name"                   : str
        - "Course Description"            : str
        - "Class Number"                  : str
        - "Section"                       : str
        - "Instructor"                    : str
        - "Enrollment"                    : str
        - "Course Date"                   : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """

        course_blogs = response.xpath('//div[@class="results"]//table[@class="courseHeaderTable"]')
        rows = []

        for course_blog in course_blogs:
            title = course_blog.xpath('./tr[@class="courseHeaderRow"]/td/text()').get("").strip()
            description = course_blog.xpath('./tr[@class="descDiv"]/td/text()').get("").strip()
            safe_name = xpath_literal(title)

            table_blogs = course_blog.xpath(
                f'./tr/td[contains(normalize-space(.), {safe_name})]'
                '/parent::tr/parent::table/following-sibling::table'
            )

            # per-class state
            class_number = None
            course_date = ""
            instructor = ""
            location = ""
            seats = None
            quota = None

            for table_blog in table_blogs:
                if table_blog.xpath('./@class').get("") == "courseHeaderTable":
                    break

                # detect new class
                new_class_number = table_blog.xpath(
                    './/tr[@class="courseSectionRow2_2"]/td[1]/text()'
                ).get()

                if new_class_number:
                    # save previous class
                    if class_number and seats is not None and quota is not None:
                        enrollment = f"{quota - seats}/{quota}"

                        rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Course Name": title,
                            "Course Description": description,
                            "Class Number": class_number,
                            "Section": "",
                            "Instructor": instructor,
                            "Enrollment": enrollment,
                            "Course Dates": course_date,
                            "Location": location,
                            "Textbook/Course Materials": "",
                        })

                    # reset for new class
                    class_number = new_class_number.strip()
                    course_date = table_blog.xpath(
                        './/tr[@class="courseSectionRow2_2"]/td[4]/text()'
                    ).get("").strip()
                    location = table_blog.xpath(
                        './/tr[@class="courseSectionRow2_2"]/td[5]/text()'
                    ).get("").strip()

                    instructor = ""
                    seats = None
                    quota = None

                # seats & quota
                seats_text = table_blog.xpath(
                    './/strong[contains(text(),"Seats Still Available")]/parent::td/text()'
                ).get("")
                quota_text = table_blog.xpath(
                    './/strong[contains(text(),"Class Quota")]/parent::td/text()'
                ).get("")

                if seats_text:
                    seats = safe_int(seats_text)
                if quota_text:
                    quota = safe_int(quota_text)

                # instructor
                instr = table_blog.xpath(
                    './/strong[contains(text(),"Instructor")]/parent::td/text()'
                ).get("")
                if instr:
                    instructor = instr.strip()

            # append LAST class
            if class_number and seats is not None and quota is not None:
                enrollment = f"{quota - seats}/{quota}"

                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Course Name": title,
                    "Course Description": description,
                    "Class Number": class_number,
                    "Section": "",
                    "Instructor": instructor,
                    "Enrollment": enrollment,
                    "Course Dates": course_date,
                    "Location": location,
                    "Textbook/Course Materials": "",
                })

        if rows:
            course_df = pd.DataFrame(rows)
            save_df(course_df, self.institution_id, "course")

    def parse_directory(self,response):

        """
        Parse directory using Scrapy response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Name"                          : str
        - "Title"                         : str
        - "Email"                         : str
        - "Phone Number"                  : str
        """

        directory_blogs = response.xpath('//table//tbody/tr')
        rows =[]
        for directory_blog in directory_blogs:
            rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": directory_blog.xpath('./td[1]/text()').get("").strip(),
                "Title": directory_blog.xpath('./td[2]/text()').get("").strip(),
                "Email": "",
                "Phone Number": directory_blog.xpath('./td[3]/text()').get("").strip(),
            })
            
        if rows:
            directory_df = pd.DataFrame(rows)
            save_df(directory_df, self.institution_id, "campus")

    @inline_requests
    def parse_calendar(self, response):

        """
        Parse calendar using Scrapy response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """

        import re
        import fitz
        import pandas as pd
        import scrapy

        MONTH_RE = re.compile(
            r"^(January|February|March|April|May|June|July|August|September|October|November|December)$",
            re.I
        )

        MONTH_YEAR_RE = re.compile(
    r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}$",
    re.I
)

        DATE_LINE_RE = re.compile(
                r'^\d{1,2}(-\d{1,2})?$'          # single day or simple range like 28-29
                r'|^\d{1,2}/\d{1,2}\s*-\s*\d{1,2}/\d{1,2}$'  # date ranges like 5/30 - 6/2
                r'|^\d+(\s*&\s*\d+)+$'           # multi-day like 21 22 23
            )

        TBD_SPLIT_RE = re.compile(r'\bTBD\b', re.I)

        pdf_links = response.xpath(
            '//div[@class="fusion-text fusion-text-1"]//ul/li/a[contains(text(),"2027") or contains(text(),"2026")]/@href'
        ).getall()

        final_rows = []
        for pdf_url in pdf_links:
            pdf_response = yield scrapy.Request(url=pdf_url)

            term_name = pdf_response.url.split("/")[-1].replace(".pdf", "").replace("%20", " ")

            meta = {
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": pdf_response.url,
                "Term Name": f"Academic Calendar {term_name}",
            }

            with fitz.open(stream=pdf_response.body, filetype="pdf") as doc:
                for page in doc:
                    width, height = page.rect.width, page.rect.height
                    columns = [
                        fitz.Rect(0, 0, width / 2, height),
                        fitz.Rect(width / 2, 0, width, height)
                    ]

                    # 🔒 MONTH IS PAGE-SCOPED, NOT COLUMN-SCOPED
                    current_month = None
                    previous_date_line = None
                    description_buffer = []

                    for rect in columns:
                        text = page.get_text("text", clip=rect)

                        if not text:
                            continue

                        lines = [l.strip() for l in text.split("\n") if l.strip()]
       
                        for line in lines:
                            lower = line.lower()

                            # Month header 
                            month_match = MONTH_RE.match(line)
                            if month_match:
                                # Flush pending event BEFORE switching month
                                if previous_date_line and description_buffer:
                                    final_rows.append({
                                        **meta,
                                        "Term Date": f"{current_month} {previous_date_line}",
                                        "Term Date Description": " ".join(description_buffer)
                                    })
                                    previous_date_line = None
                                    description_buffer = []

                                current_month = month_match.group(1)
                                continue

                            # Date line 
                            if DATE_LINE_RE.match(line):
                                # Flush previous event
                                if previous_date_line and description_buffer:
                                    full_desc = " ".join(description_buffer)

                                    if TBD_SPLIT_RE.search(full_desc):
                                        for part in TBD_SPLIT_RE.split(full_desc):
                                            desc = part.strip(" ,-&")
                                            if desc:
                                                final_rows.append({
                                                    **meta,
                                                    "Term Date": "TBD",
                                                    "Term Date Description": desc
                                                })
                                    else:
                                        final_rows.append({
                                            **meta,
                                            "Term Date": f"{current_month} {previous_date_line}",
                                            "Term Date Description": full_desc
                                        })

                                    description_buffer = []

                                previous_date_line = line
                                continue

                            # Noise
                            if lower in {"su", "m", "tu", "w", "th", "f", "sa"}:
                                continue

                            if re.fullmatch(r"(m\s+tu\s+w\s+th|su|f|sa)", lower):
                                continue

                            if lower.startswith("rev"):
                                continue

                            # Description 

                            if previous_date_line and re.search(r"[A-Za-z]", line):
                                clean_line = line.strip()

                                # 🚫 Block month headers AND month+year headers
                                if MONTH_RE.match(clean_line):
                                    continue

                                if MONTH_YEAR_RE.match(clean_line):
                                    continue

                                description_buffer.append(clean_line)
                                continue


                    # Final page flush 
                    if previous_date_line and description_buffer:
                        final_rows.append({
                            **meta,
                            "Term Date": f"{current_month} {previous_date_line}",
                            "Term Date Description": " ".join(description_buffer)
                        })

        if final_rows:
            calendar_df = pd.DataFrame(final_rows)
            save_df(calendar_df, self.institution_id, "calendar")
