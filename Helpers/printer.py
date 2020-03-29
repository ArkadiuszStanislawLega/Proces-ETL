class SpecialPrinter:
    """
    The class responsible for printing special types of text to the console.
    """
    @staticmethod
    def surrounded_text(text: str, length: int, midSing=">", surroundingSign="="):
        """
        Enters the text in the center in the console of the specified length surrounded by underscores.
        :param text: The text to be in the middle.
        :param length: Total length including text.
        :param sing: The character to be between the beginning and the text and between the text and the end of the string.
        :param surroundingSign: The character to be between the beginning and the text and between the text and the end of the string.
        :return: None.
        """
        if len(f' {text} ') <= length and len(midSing) == 1 and len(surroundingSign) == 1:
            # Check the length of the word to be entered
            numberOfChars = len(f' {text} ')

            # Check if the word size is even
            if numberOfChars % 2 == 0:
                isHeEven = True
            else:
                isHeEven = False

            # Divide all characters in half
            midle = length/2
            # Divide the characters to be in the middle in half
            halfOfChars = numberOfChars/2
            # Count how many characters you want before the first letter
            numberOfSigns = midle - halfOfChars
            roundedNumberOfSigns = round(numberOfSigns)

            if surroundingSign != ' ':
                print(f'{length * surroundingSign}')

            # region Printed value
            if isHeEven:
                print(
                    f'{roundedNumberOfSigns * midSing} {text} {(roundedNumberOfSigns) * midSing}')
            else:
                chackValue = (2*roundedNumberOfSigns) + (numberOfChars - 1)

                if(chackValue == length):
                    print(
                        f'{roundedNumberOfSigns * midSing} {text} {(roundedNumberOfSigns - 1) * midSing}')
                else:
                    if (chackValue > length):
                        difference = chackValue - length

                    elif (chackValue < length):
                        difference = (length - chackValue) / 2

                    print(
                        f'{roundedNumberOfSigns * midSing} {text} {(roundedNumberOfSigns + int(difference) ) * midSing}')
            # endregion
            if (surroundingSign != ' '):
                print(f'{length * surroundingSign}')
        else:
            print(text)
